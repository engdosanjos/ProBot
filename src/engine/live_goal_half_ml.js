// src/engine/live_goal_half_ml.js
const path = require('path');
const Better = require('better-sqlite3');
const fs = require('fs');
const db = require('../db');

const LOOKBACK_MIN = Number(process.env.PRESS_LOOKBACK_MIN || 6);
const ML_URL = process.env.ML_URL || 'http://127.0.0.1:8009/predict';

const HT_MAX_MINUTE = 35;
const FT_MAX_MINUTE = 80;

const MODELS_DIR = process.env.MODELS_DIR || path.join(__dirname, '..', '..', 'models');
const THRESH_PATH = path.join(MODELS_DIR, 'thresholds.json');
let THR = { ht: { threshold: 0.55 }, ft: { threshold: 0.55 } };
try {
  THR = JSON.parse(fs.readFileSync(THRESH_PATH, 'utf8'));
} catch { /* usa default 0.55 */ }

// ===== leitura direta por minuto =====
const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', '..', 'data', 'events.db');
let raw;
function ensureRaw() {
  if (!raw) {
    raw = new Better(DB_PATH);
    raw.pragma('journal_mode = WAL');
    raw.pragma('busy_timeout = 5000');
  }
  return raw;
}
const qWin = () => ensureRaw().prepare(`
  SELECT * FROM ticks
   WHERE event_id = ?
     AND minute IS NOT NULL
     AND minute >= ? AND minute <= ?
   ORDER BY minute ASC, ts ASC
`);
const qLast = () => ensureRaw().prepare(`SELECT * FROM ticks WHERE event_id=? ORDER BY ts DESC LIMIT 1`);

// ===== features (mesma lógica do treino/calibração) =====
function num(v){ return Number.isFinite(v) ? v : 0; }
function buildFeatures(rows, minute) {
  if (!rows || !rows.length) return {};
  const mFrom = Math.max(0, minute - LOOKBACK_MIN);
  const win = rows.filter(r => (r.minute||0) >= mFrom && (r.minute||0) <= minute);
  const base = win.length ? win[0] : rows[0];
  const last = win.length ? win[win.length-1] : rows[rows.length-1];

  const dpair = (h,a) => [ num(last[h])-num(base[h]), num(last[a])-num(base[a]) ];
  const [d_sot_h, d_sot_a] = dpair('sot_home','sot_away');
  const [d_sof_h, d_sof_a] = dpair('soff_home','soff_away');
  const [d_da_h,  d_da_a ] = dpair('da_home','da_away');
  const [d_co_h,  d_co_a ] = dpair('corners_home','corners_away');

  const press_home = 3*d_sot_h + 1.5*d_sof_h + 0.5*d_da_h + 0.5*d_co_h;
  const press_away = 3*d_sot_a + 1.5*d_sof_a + 0.5*d_da_a + 0.5*d_co_a;

  const feat = {};
  feat.minute = num(minute);
  feat.goal_diff = num(last.goals_home) - num(last.goals_away);
  feat.press_home = num(press_home);
  feat.press_away = num(press_away);

  feat.d_sot_home = num(d_sot_h);
  feat.d_sot_away = num(d_sot_a);
  feat.d_soff_home = num(d_sof_h);
  feat.d_soff_away = num(d_sof_a);
  feat.d_corners_home = num(d_co_h);
  feat.d_corners_away = num(d_co_a);
  feat.d_da_home = num(d_da_h);
  feat.d_da_away = num(d_da_a);

  const keys = ['st_home','st_away','sot_home','sot_away','soff_home','soff_away',
                'da_home','da_away','corners_home','corners_away','goals_home','goals_away'];
  for (const k of keys) feat['cum_'+k] = num(last[k]);

  return feat;
}

// ===== emitir sinais HT/FT =====
async function httpPost(url, data) {
  const res = await fetch(url, {
    method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify(data)
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.json();
}

function hasOpenSide(event_id, side) {
  const open = db.getOpenSignals(event_id) || [];
  return open.some(s => s.side === side);
}

async function onTickAnalyze(ctx) {
  // ctx: { event_id, minute, league, home, away }
  if (!Number.isFinite(ctx.minute)) return;
  const rows = qWin().all(ctx.event_id, Math.max(0, ctx.minute - LOOKBACK_MIN), ctx.minute);
  if (!rows.length) return;

  const features = buildFeatures(rows, ctx.minute);
  let p_ht=0, p_ft=0;
  try {
    const r = await httpPost(ML_URL, { features });
    p_ht = Number(r.p_ht || 0); p_ft = Number(r.p_ft || 0);
  } catch (e) {
    return; // silencioso
  }

  // HT (até 35')
  if (ctx.minute <= HT_MAX_MINUTE && p_ht >= (THR?.ht?.threshold ?? 0.55) && !hasOpenSide(ctx.event_id, 'HT')) {
    db.insertSignal({
      event_id: ctx.event_id, league: ctx.league, home: ctx.home, away: ctx.away,
      created_ts: Date.now(), minute: ctx.minute, window: 45, side: 'HT',
      prob: p_ht, p_global: null, p_league: null, p_team: null,
      expire_minute: 45
    });
  }

  // FT (até 80')
  if (ctx.minute <= FT_MAX_MINUTE && p_ft >= (THR?.ft?.threshold ?? 0.55) && !hasOpenSide(ctx.event_id, 'FT')) {
    db.insertSignal({
      event_id: ctx.event_id, league: ctx.league, home: ctx.home, away: ctx.away,
      created_ts: Date.now(), minute: ctx.minute, window: 90, side: 'FT',
      prob: p_ft, p_global: null, p_league: null, p_team: null,
      expire_minute: 90
    });
  }
}

module.exports = { onTickAnalyze };
