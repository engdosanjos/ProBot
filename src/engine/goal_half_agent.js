// src/engine/goal_half_agent.js
const fs = require('fs');
const path = require('path');
const db = require('../db');

const ML_URL = process.env.ML_URL || 'http://127.0.0.1:8000/predict';
const COOLDOWN_MIN = Number(process.env.COOLDOWN_MIN || 3);
const HT_MAX_MINUTE = Number(process.env.HT_MAX_MINUTE || 35);
const FT_MAX_MINUTE = Number(process.env.FT_MAX_MINUTE || 80);

let thresholds = { ht_threshold: 0.6, ft_threshold_before50: 0.6, ft_threshold_after50: 0.6 };
try {
  const f = path.join(process.cwd(), 'models', 'thresholds.json');
  if (fs.existsSync(f)) thresholds = JSON.parse(fs.readFileSync(f, 'utf8'));
} catch {}

const lastSignalMin = new Map(); // event_id -> minute

function buildHist(event_id, uptoTs) {
  const rows = db.getTicksRange(event_id, 0, uptoTs) || [];
  // compacta por minuto (pega último daquele minuto)
  const byMin = new Map();
  for (const r of rows) {
    if (r.minute == null) continue;
    byMin.set(r.minute, r);
  }
  const out = Array.from(byMin.values()).sort((a, b) => a.minute - b.minute).map(r => ({
    minute: r.minute,
    st_home: r.st_home||0, st_away: r.st_away||0,
    sot_home: r.sot_home||0, sot_away: r.sot_away||0,
    soff_home: r.soff_home||0, soff_away: r.soff_away||0,
    corners_home: r.corners_home||0, corners_away: r.corners_away||0,
    da_home: r.da_home||0, da_away: r.da_away||0,
    goals_home: r.goals_home||0, goals_away: r.goals_away||0,
  }));
  // mantém só os últimos 25-30 minutos para reduzir payload
  return out.slice(-30);
}

async function callPredict(ctx, hist) {
  const body = JSON.stringify({
    event_id: ctx.event_id, league: ctx.league||'', home: ctx.home||'', away: ctx.away||'', hist
  });
  const r = await fetch(ML_URL, { method:'POST', headers:{'Content-Type':'application/json'}, body });
  return await r.json();
}

function canEmit(event_id, minute) {
  const last = lastSignalMin.get(event_id) || -999;
  return (minute - last) >= COOLDOWN_MIN;
}
function markEmit(event_id, minute) {
  lastSignalMin.set(event_id, minute);
}

// decide e grava na tabela signals (usando side 'HT'/'FT' e expire_minute 45/90)
async function maybeSignal(ctx, proba) {
  const minute = ctx.minute || 0;

  // evita duplicação
  const opens = db.getOpenSignals(ctx.event_id) || [];
  const hasHT = opens.some(s => s.side === 'HT');
  const hasFT = opens.some(s => s.side === 'FT');

  // HT
  if (minute <= HT_MAX_MINUTE && !hasHT && canEmit(ctx.event_id, minute)) {
    if (proba.p_ht >= thresholds.ht_threshold) {
      db.insertSignal({
        event_id: ctx.event_id,
        league: ctx.leagueKey || ctx.league || '',
        home: ctx.homeKey || ctx.home || '',
        away: ctx.awayKey || ctx.away || '',
        created_ts: Date.now(),
        minute,
        window: 45 - minute,            // só para preencher
        side: 'HT',
        prob: proba.p_ht,
        p_global: null, p_league: null, p_team: null,
        expire_minute: 45
      });
      markEmit(ctx.event_id, minute);
    }
  }

  // FT
  if (minute <= FT_MAX_MINUTE && !hasFT && canEmit(ctx.event_id, minute)) {
    const thr = minute >= 50 ? (thresholds.ft_threshold_after50 ?? 0.6)
                             : (thresholds.ft_threshold_before50 ?? 0.6);
    if (proba.p_ft >= thr) {
      db.insertSignal({
        event_id: ctx.event_id,
        league: ctx.leagueKey || ctx.league || '',
        home: ctx.homeKey || ctx.home || '',
        away: ctx.awayKey || ctx.away || '',
        created_ts: Date.now(),
        minute,
        window: 90 - minute,           // só para preencher
        side: 'FT',
        prob: proba.p_ft,
        p_global: null, p_league: null, p_team: null,
        expire_minute: 90
      });
      markEmit(ctx.event_id, minute);
    }
  }
}

async function onTick(ctx) {
  // ctx: {event_id, league, home, away, minute, ts}
  if (!Number.isFinite(ctx.minute)) return;
  const hist = buildHist(ctx.event_id, ctx.ts || Date.now());
  if (!hist.length) return;

  try {
    const res = await callPredict(ctx, hist);
    if (!res || !res.ok) return;
    await maybeSignal(ctx, { p_ht: res.p_ht, p_ft: res.p_ft });
  } catch (e) {
    // silencioso
  }
}

module.exports = { onTick };
