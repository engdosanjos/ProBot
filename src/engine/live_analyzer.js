// src/engine/live_analyzer.js
// Predição HT/FT sem janela — emite sinais com expire_minute=45 ou 90.

const db = require('../db');
const { featurizeForMinute } = require('./featurize');
const { predictGoalProbs } = require('./ml_infer');

// —— Política ——
const THRESH_HT = Number(process.env.P_HT || 0.62);
const THRESH_FT = Number(process.env.P_FT || 0.58);

const MAX_MIN_HT = Number(process.env.MAX_MIN_HT || 35); // não entrar depois de 35'
const MAX_MIN_FT = Number(process.env.MAX_MIN_FT || 80); // não entrar depois de 80'
const MIN_MIN_HT = Number(process.env.MIN_MIN_HT || 8);  // evita muito cedo
const MIN_MIN_FT = Number(process.env.MIN_MIN_FT || 48); // FT depois do intervalo

const MIN_DOM6   = Number(process.env.MIN_DOM6   || 3.0); // dominância mínima
const MIN_SLOPE6 = Number(process.env.MIN_SLOPE6 || 0.15); // rampa mínima

const COOLDOWN_MIN      = Number(process.env.COOLDOWN_MIN || 3); // não spam
const POST_GOAL_COOLDOWN= Number(process.env.POST_GOAL_COOLDOWN || 2);

// estado efêmero (memória) p/ cooldowns
const lastSignalByEvent = new Map(); // key: `${event_id}:${side}` -> minute
const lastGoalByEvent   = new Map(); // key: event_id -> minute

function updateLastGoal(event_id, histAsc) {
  const lg = (function() {
    let last = null;
    for (let i = 1; i < histAsc.length; i++) {
      const p = histAsc[i-1], c = histAsc[i];
      const gp = (p.goals_home||0)+(p.goals_away||0);
      const gc = (c.goals_home||0)+(c.goals_away||0);
      if (gc > gp) last = c.minute ?? last;
    }
    return last;
  })();
  if (lg != null) lastGoalByEvent.set(event_id, lg);
}

function canEnter(side, minute, aux) {
  const afterGoal = lastGoalByEvent.get(aux.event_id);
  const sinceGoal = Number.isFinite(afterGoal) ? (minute - afterGoal) : 999;

  if (side === 'HT') {
    if (minute < MIN_MIN_HT || minute > MAX_MIN_HT) return false;
  } else {
    if (minute < MIN_MIN_FT || minute > MAX_MIN_FT) return false;
  }
  if (aux.dom6 < MIN_DOM6) return false;
  if (aux.slope6 < MIN_SLOPE6) return false;
  if (sinceGoal < POST_GOAL_COOLDOWN) return false;

  const key = `${aux.event_id}:${side}`;
  const lastS = lastSignalByEvent.get(key);
  if (Number.isFinite(lastS) && (minute - lastS) < COOLDOWN_MIN) return false;

  return true;
}

async function onTickAnalyze(ctx) {
  if (!Number.isFinite(ctx.minute)) return;

  // 1) histórico e features
  const nowTs = ctx.ts || Date.now();
  const hist = db.getTicksRange(ctx.event_id, 0, nowTs) || [];
  if (!hist.length) return;

  const curr = hist[hist.length - 1];
  updateLastGoal(ctx.event_id, hist);

  const { features, aux } = featurizeForMinute(ctx.event_id, curr, hist);
  aux.event_id = ctx.event_id;

  // 2) previsão P(gol até o fim do tempo atual)
  const { p_ht, p_ft } = await predictGoalProbs(features);

  // 3) salvar previsões para o painel (dois “windows” especiais 45 e 90 só para mostrar)
  db.insertPrediction({
    event_id: ctx.event_id, ts: nowTs,
    league: ctx.league || '', home: ctx.home || '', away: ctx.away || '',
    minute: ctx.minute || 0, window_min: 45, prob: +(p_ht.toFixed(4))
  });
  db.insertPrediction({
    event_id: ctx.event_id, ts: nowTs,
    league: ctx.league || '', home: ctx.home || '', away: ctx.away || '',
    minute: ctx.minute || 0, window_min: 90, prob: +(p_ft.toFixed(4))
  });

  // 4) decidir entradas
  const minute = ctx.minute || 0;

  // HT
  if (minute <= 45) {
    if (p_ht >= THRESH_HT && canEnter('HT', minute, aux)) {
      // evitar duplicata: já existe sinal HT aberto?
      const open = (db.getOpenSignals(ctx.event_id) || []).some(s => (s.side === 'HT'));
      if (!open) {
        db.insertSignal({
          event_id: ctx.event_id,
          league: ctx.league || '', home: ctx.home || '', away: ctx.away || '',
          created_ts: nowTs, minute,
          window: 0, side: 'HT',
          prob: p_ht, p_global: null, p_league: null, p_team: null,
          expire_minute: 45
        });
        lastSignalByEvent.set(`${ctx.event_id}:HT`, minute);
      }
    }
  } else {
    // FT
    if (p_ft >= THRESH_FT && canEnter('FT', minute, aux)) {
      const open = (db.getOpenSignals(ctx.event_id) || []).some(s => (s.side === 'FT'));
      if (!open) {
        db.insertSignal({
          event_id: ctx.event_id,
          league: ctx.league || '', home: ctx.home || '', away: ctx.away || '',
          created_ts: nowTs, minute,
          window: 0, side: 'FT',
          prob: p_ft, p_global: null, p_league: null, p_team: null,
          expire_minute: 90
        });
        lastSignalByEvent.set(`${ctx.event_id}:FT`, minute);
      }
    }
  }

  // 5) assentar (fecha green/red no gol ou expiração 45/90)
  settleOnline(ctx);
}

// ——— Settler (mesmo do teu arquivo anterior, fracão extraído para cá) ———
const Better = require('better-sqlite3');
let rawDb;
function ensureRaw() {
  if (!rawDb) {
    const path = require('path');
    const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', '..', 'data', 'events.db');
    rawDb = new Better(DB_PATH);
    rawDb.pragma('journal_mode = WAL');
    rawDb.pragma('busy_timeout', 5000);
  }
  return rawDb;
}
const qLastTwoTicks = () => ensureRaw().prepare(`SELECT * FROM ticks WHERE event_id = ? ORDER BY ts DESC LIMIT 2`);

function settleOnline(ctx) {
  const two = qLastTwoTicks().all(ctx.event_id);
  if (!two || two.length < 2) return;
  const curr = two[0], prev = two[1];

  const gPrev = (prev.goals_home || 0) + (prev.goals_away || 0);
  const gCurr = (curr.goals_home || 0) + (curr.goals_away || 0);
  const goalDelta = gCurr - gPrev;
  const minuteNow = curr.minute || ctx.minute || 0;

  const signals = db.getOpenSignals(ctx.event_id) || [];
  for (const s of signals) {
    let needSettle = false;
    let status = null, result = null, pnl = 0;

    if (goalDelta > 0 && minuteNow <= (s.expire_minute || 0)) {
        needSettle = true; status = 'won'; result = 'green';
        const HT_WIN = Number(process.env.HT_WIN_UNIT || 1.2);
        const FT_PRE = Number(process.env.FT_WIN_UNIT_PRE || 1.1);
        const FT_POST = Number(process.env.FT_WIN_UNIT_POST || 1.2);
        if (s.side === 'HT') pnl = HT_WIN;
        else if (s.side === 'FT') pnl = ( (s.minute || 0) >= 50 ? FT_POST : FT_PRE );
        else pnl = Number(process.env.WIN_UNIT || 0.1);
    } else if (minuteNow >= (s.expire_minute || 0)) {
        needSettle = true; status = 'lost'; result = 'red';
        pnl = -Math.abs(Number(process.env.LOSE_UNIT || 1.0));
    }
  }
}

module.exports = { onTickAnalyze };
