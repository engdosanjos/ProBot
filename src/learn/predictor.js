// src/learn/predictor.js
const db = require('../db');
const { bucketMinute, binPdom, binPsum, binGd } = require('./features');
const { WINDOWS } = require('./learner');

const K_GLOBAL = Number(process.env.K_GLOBAL || 500); // quanto “peso” precisa p/ dominar
const K_LEAGUE = Number(process.env.K_LEAGUE || 200);
const K_TEAM   = Number(process.env.K_TEAM   || 100);

// thresholds para sinalizar aposta (ajuste depois)
const THRESH = {
  10: Number(process.env.THRESH_W10 || 0.58),
  15: Number(process.env.THRESH_W15 || 0.57),
  20: Number(process.env.THRESH_W20 || 0.56),
  25: Number(process.env.THRESH_W25 || 0.56),
  30: Number(process.env.THRESH_W30 || 0.55),
  HT: Number(process.env.THRESH_HT  || 0.55),
  FT: Number(process.env.THRESH_FT  || 0.55),
};

function fetchProb(scope_type, scope_key, window, side, f) {
  const row = db.getModelRow(scope_type, scope_key, window, side, f.pdom_bin, f.psum_bin, f.m_bucket, f.gd_bin);
  const n = row?.n || 0;
  const y = row?.y || 0;
  // Laplace
  const alpha = 1;
  const p = (y + alpha) / (n + 2 * alpha);
  return { p, n };
}

function blendProbs({ league, teamKey, window, side, f }) {
  const g = fetchProb('global', '*', window, side, f);
  const l = fetchProb('league', league || '', window, side, f);
  const t = fetchProb('team', teamKey || '', window, side, f);

  const wg = Math.min(1, g.n / K_GLOBAL);
  const wl = Math.min(1, l.n / K_LEAGUE);
  const wt = Math.min(1, t.n / K_TEAM);

  // mistura normalizada (garante soma>0)
  const sum = (wg || 0) + (wl || 0) + (wt || 0) || 1;
  const p = (g.p * (wg || 0) + l.p * (wl || 0) + t.p * (wt || 0)) / sum;

  return { p, parts: { global: g.p, league: l.p, team: t.p } };
}

function probabilitiesNow({ league, home, away, minute, press_home, press_away, gd }) {
  const f = {
    pdom_bin: binPdom((press_home || 0) - (press_away || 0)),
    psum_bin: binPsum((press_home || 0) + (press_away || 0)),
    m_bucket: bucketMinute(minute || 0),
    gd_bin: binGd(gd || 0),
  };

  const out = { windows: {}, HT: null, FT: null };

  for (const w of WINDOWS) {
    const homeP = blendProbs({ league, teamKey: home, window: w, side: 'home', f });
    const awayP = blendProbs({ league, teamKey: away, window: w, side: 'away', f });
    // qualquer gol
    const any = blendProbs({ league, teamKey: '', window: w, side: 'any', f });
    // também poderíamos combinar 1-(1-ph)*(1-pa), mas mantemos 'any' direta para treinar isso também
    out.windows[w] = {
      home: homeP,
      away: awayP,
      any: any
    };
  }

  // HT/FT tomam janela até 45 / 90
  const anyHT = blendProbs({ league, teamKey: '', window: 45, side: 'any', f });
  const anyFT = blendProbs({ league, teamKey: '', window: 90, side: 'any', f });
  out.HT = anyHT;
  out.FT = anyFT;

  return out;
}

function maybeSignal({ event_id, league, home, away, minute, probs }) {
  const now = Date.now();
  for (const w of Object.keys(probs.windows)) {
    const p = probs.windows[w].any; // entrada “gol de qualquer lado”
    const th = THRESH[w];
    if (p.p >= th) {
      db.insertSignal({
        event_id, league, home, away,
        created_ts: now, minute, window: Number(w), side: 'any',
        prob: p.p, p_global: p.parts.global, p_league: p.parts.league, p_team: p.parts.team,
        expire_minute: minute + Number(w)
      });
    }
  }
  if (probs.HT?.p >= THRESH.HT) {
    db.insertSignal({
      event_id, league, home, away,
      created_ts: now, minute, window: 45, side: 'HT',
      prob: probs.HT.p, p_global: probs.HT.parts.global, p_league: probs.HT.parts.league, p_team: probs.HT.parts.team,
      expire_minute: 45
    });
  }
  if (probs.FT?.p >= THRESH.FT) {
    db.insertSignal({
      event_id, league, home, away,
      created_ts: now, minute, window: 90, side: 'FT',
      prob: probs.FT.p, p_global: probs.FT.parts.global, p_league: probs.FT.parts.league, p_team: probs.FT.parts.team,
      expire_minute: 90
    });
  }
}

function settleSignalsOnTick({ event_id, minute, goals_home, goals_away, last_goals_home, last_goals_away }) {
  const open = db.getOpenSignals(event_id);
  const scorer =
    (goals_home > last_goals_home) ? 'home' :
    (goals_away > last_goals_away) ? 'away' : null;

  for (const s of open) {
    let win = false;
    if (s.side === 'any') {
      if (scorer && minute <= s.expire_minute) win = true;
    } else if (s.side === 'HT') {
      if (minute <= 45 && scorer) win = true;
    } else if (s.side === 'FT') {
      if (minute <= 90 && scorer) win = true;
    }
    if (win) {
      db.settleSignal({ id: s.id, status: 'won', settle_minute: minute, result: 'green', pnl: 0.1 });
    } else if (minute > s.expire_minute || (s.side === 'HT' && minute > 45) || (s.side === 'FT' && minute > 90)) {
      db.settleSignal({ id: s.id, status: 'lost', settle_minute: Math.min(minute, s.expire_minute), result: 'red', pnl: -1.0 });
    }
  }
}

module.exports = { probabilitiesNow, maybeSignal, settleSignalsOnTick, THRESH };
