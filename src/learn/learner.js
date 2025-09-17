// src/learn/learner.js
const db = require('../db');
const { pressureFromDeltas, bucketMinute, binPdom, binPsum, binGd } = require('./features');

const WINDOWS = [10, 15, 20, 25, 30];          // minutos
const SAMPLE_EVERY_MINUTE = true;              // 1 amostra/min por jogo
const ALPHA = 1;                               // suavização Laplace (usado na predição)

const lastByEvent = new Map(); // para detectar gol e limitar 1 amostra/min

function goalSide(prev, curr) {
  if (prev == null) return null;
  if ((curr.goals_home ?? 0) > (prev.goals_home ?? 0)) return 'home';
  if ((curr.goals_away ?? 0) > (prev.goals_away ?? 0)) return 'away';
  return null;
}

function queueSamples({ event_id, league, home, away, minute, deltas, goals_home, goals_away }) {
  const last = lastByEvent.get(event_id);
  if (SAMPLE_EVERY_MINUTE && last && last.minute === minute) return; // já amostrado este minuto

  const gd = (goals_home || 0) - (goals_away || 0);
  const { ph, pa } = pressureFromDeltas(
    { sot_home: deltas?.sot_home||0, soff_home: deltas?.soff_home||0, da_home: deltas?.da_home||0, corners_home: deltas?.corners_home||0 },
    { sot_away: deltas?.sot_away||0, soff_away: deltas?.soff_away||0, da_away: deltas?.da_away||0, corners_away: deltas?.corners_away||0 }
  );
  const pdom = ph - pa;
  const psum = ph + pa;

  const base = {
    event_id, league, home, away,
    created_ts: Date.now(),
    minute,
    pdom_bin: binPdom(pdom),
    psum_bin: binPsum(psum),
    m_bucket: bucketMinute(minute),
    gd_bin: binGd(gd)
  };

  // janelas relativas
  for (const w of WINDOWS) {
    const expire = minute + w;
    db.addPending({ ...base, window: w, side: 'home', expire_minute: expire });
    db.addPending({ ...base, window: w, side: 'away', expire_minute: expire });
    db.addPending({ ...base, window: w, side: 'any',  expire_minute: expire });
  }
  // HT / FT (absolutas)
  const toHT = Math.max(0, 45 - minute);
  const toFT = Math.max(0, 90 - minute);
  db.addPending({ ...base, window: 45, side: 'any', expire_minute: 45 });
  db.addPending({ ...base, window: 90, side: 'any', expire_minute: 90 });

  lastByEvent.set(event_id, { minute, goals_home, goals_away });
}

function settleOutcomesOnGoal({ event_id, league, home, away, minute, scorer_side }) {
  const pendings = db.getOpenPendings(event_id);
  for (const p of pendings) {
    if (p.settled) continue;
    // vitória: se o gol ocorreu antes do expirar e o lado bate
    const win =
      minute <= p.expire_minute &&
      (p.side === 'any' || p.side === scorer_side) ? 1 : 0;

    // atualiza contadores do modelo nos 3 níveis
    const bumps = [
      { scope_type: 'global', scope_key: '*' },
      { scope_type: 'league', scope_key: p.league || '' },
    ];
    // nível time: se side é 'home' conta para time da casa; se 'away', para visitante; se 'any', conta para os dois
    if (p.side === 'home' || p.side === 'any') bumps.push({ scope_type: 'team', scope_key: p.home || '' });
    if (p.side === 'away' || p.side === 'any') bumps.push({ scope_type: 'team', scope_key: p.away || '' });

    for (const b of bumps) {
      db.bumpModel({
        ...b,
        window: p.window,
        side: p.side,
        pdom_bin: p.pdom_bin,
        psum_bin: p.psum_bin,
        m_bucket: p.m_bucket,
        gd_bin: p.gd_bin,
        n: 1,
        y: win
      });
    }

    db.settlePending({ id: p.id, won: win, settle_minute: minute, scorer_side });
  }
}

function expireLost(event_id, currentMinute) {
  const pendings = db.getOpenPendings(event_id);
  for (const p of pendings) {
    if (p.settled) continue;
    if (currentMinute > p.expire_minute) {
      // perdeu por tempo
      const bumps = [
        { scope_type: 'global', scope_key: '*' },
        { scope_type: 'league', scope_key: p.league || '' },
      ];
      if (p.side === 'home' || p.side === 'any') bumps.push({ scope_type: 'team', scope_key: p.home || '' });
      if (p.side === 'away' || p.side === 'any') bumps.push({ scope_type: 'team', scope_key: p.away || '' });

      for (const b of bumps) {
        db.bumpModel({
          ...b,
          window: p.window,
          side: p.side,
          pdom_bin: p.pdom_bin,
          psum_bin: p.psum_bin,
          m_bucket: p.m_bucket,
          gd_bin: p.gd_bin,
          n: 1,
          y: 0
        });
      }
      db.settlePending({ id: p.id, won: 0, settle_minute: p.expire_minute, scorer_side: null });
    }
  }
}

function onTickLearn({ event_id, league, home, away, minute, goals_home, goals_away, deltas }) {
  // 1) enfileira amostras (1/min)
  queueSamples({ event_id, league, home, away, minute, deltas, goals_home, goals_away });

  // 2) detecta gol
  const last = lastByEvent.get(event_id);
  const side = goalSide(last, { goals_home, goals_away });
  if (side) {
    settleOutcomesOnGoal({ event_id, league, home, away, minute, scorer_side: side });
  }

  // 3) expira perdidas por tempo
  expireLost(event_id, minute);

  // 4) atualiza last
  lastByEvent.set(event_id, { minute, goals_home, goals_away });
}

module.exports = { onTickLearn, WINDOWS, ALPHA };
