// src/engine/train_policy.js
require('dotenv').config();
const path = require('path');
const Better = require('better-sqlite3');
const db = require('../db');
const {
  normalizeLeagueName, normalizeTeamName,
  binMinuteBucket, binGoalDiff,
  buildFeatureSignatureFromRows, fnv1a32,
} = require('./model_features');

const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', '..', 'data', 'events.db');

// janela base pra hazard
const BASE_W = Number(process.env.TRAIN_BASE_WINDOW || 10);

// grids simples (ajuste à vontade)
const GRID = {
  min_p:       [0.50, 0.55, 0.60, 0.65],
  uplift:      [1.2, 1.5, 1.8],
  dom:         [1, 2, 3],
  ramp:        [0.5, 1.0, 1.5],
  cooldown:    [1, 2, 3],
};

// mínimos para aceitar política por escopo
const MIN_SIGNALS_TEAM   = Number(process.env.MIN_SIGNALS_TEAM   || 200);
const MIN_SIGNALS_LEAGUE = Number(process.env.MIN_SIGNALS_LEAGUE || 400);
const MIN_SIGNALS_GLOBAL = Number(process.env.MIN_SIGNALS_GLOBAL || 1000);

// payouts
const WIN_HT = +Number(process.env.WIN_HT || 1.2);
const WIN_FT_AFTER50 = +Number(process.env.WIN_FT_AFTER50 || 1.2);
const WIN_FT_BEFORE50 = +Number(process.env.WIN_FT_BEFORE50 || 1.1);
const LOSE_ANY = -Math.abs(Number(process.env.LOSE_ANY || 1.0));

let raw;
function ensureRaw() {
  if (!raw) {
    raw = new Better(DB_PATH);
    raw.pragma('journal_mode = WAL');
    raw.pragma('busy_timeout = 5000');
  }
  return raw;
}

// queries
const qEvents = () => ensureRaw().prepare(`SELECT DISTINCT event_id FROM ticks WHERE minute IS NOT NULL`);
const qTicks  = () => ensureRaw().prepare(`SELECT * FROM ticks WHERE event_id = ? AND minute IS NOT NULL ORDER BY minute ASC, ts ASC`);
const qMeta   = () => ensureRaw().prepare(`SELECT league, home, away FROM matches WHERE event_id = ?`);
const qModelRow = () => ensureRaw().prepare(`
  SELECT n, y FROM model_counts
  WHERE scope_type = 'global' AND scope_key='*' AND window = ? AND side='any'
    AND pdom_bin=0 AND psum_bin=0 AND m_bucket = ? AND gd_bin = ?
`);

function estProbFromCounts(n, y, prior=0.28, alpha=50){
  return (y + alpha*prior) / (n + alpha);
}
function lookupBaselineGlobal(minute, gd){
  const m_bucket = Math.floor((minute||0)/5);
  const gd_bin = Math.max(-2, Math.min(2, gd||0));
  const r = qModelRow().get(BASE_W, m_bucket, gd_bin);
  if (!r) return 0.28;
  return estProbFromCounts(r.n||0, r.y||0);
}

function pToLambda(p, W){ if(p<=0) return 0; return -Math.log(Math.max(1e-9, 1-p))/Math.max(1,W); }
function project(pW, Wbase, horizon){
  const lam = pToLambda(pW, Wbase);
  return 1 - Math.exp(-lam * Math.max(1, horizon));
}

function pressSeries(rows){
  const out=[];
  for(let i=1;i<rows.length;i++){
    const a=rows[i-1], b=rows[i];
    const dh = {
      sot:(b.sot_home||0)-(a.sot_home||0),
      soff:(b.soff_home||0)-(a.soff_home||0),
      da:(b.da_home||0)-(a.da_home||0),
      corners:(b.corners_home||0)-(a.corners_home||0),
    };
    const da_ = {
      sot:(b.sot_away||0)-(a.sot_away||0),
      soff:(b.soff_away||0)-(a.soff_away||0),
      da:(b.da_away||0)-(a.da_away||0),
      corners:(b.corners_away||0)-(a.corners_away||0),
    };
    const ph = (dh.sot*3)+(dh.soff*1.5)+(dh.da*0.5)+(dh.corners*0.5);
    const pa = (da_.sot*3)+(da_.soff*1.5)+(da_.da*0.5)+(da_.corners*0.5);
    out.push({ m:b.minute||0, dom:(ph-pa) });
  }
  return out;
}
function rampMetric(series, lbk, mNow){
  const from = Math.max(0, mNow - lbk*2);
  const mid  = Math.max(0, mNow - lbk);
  const sel = (lo,hi)=>series.filter(s=>s.m>lo && s.m<=hi).map(s=>s.dom);
  const avg = a=>a.length? a.reduce((x,y)=>x+y,0)/a.length : 0;
  const r1 = avg(sel(mid, mNow)), r0 = avg(sel(from, mid));
  return { domNow:r1, ramp:(r1-r0) };
}
function lastGoalWithin(rows, mNow, span){
  const start = Math.max(0, mNow - span);
  let prevSum = (rows[0].goals_home||0)+(rows[0].goals_away||0);
  for (let i=1;i<rows.length;i++){
    const cur=rows[i];
    const sum=(cur.goals_home||0)+(cur.goals_away||0);
    if(cur.minute>start && cur.minute<=mNow && sum>prevSum) return true;
    prevSum = sum;
  }
  return false;
}
function goalBetween(rows, mStart, mEnd){
  let baseSum = null;
  for (let i=0;i<rows.length;i++){
    const m = rows[i].minute||0;
    const sum=(rows[i].goals_home||0)+(rows[i].goals_away||0);
    if (baseSum===null && m>=mStart){ baseSum = sum; }
    if (baseSum!==null && m>mStart && m<=mEnd && sum>baseSum) return true;
  }
  return false;
}

// simulação de política em 1 jogo → retorna {signalsHT, pnlHT, signalsFT, pnlFT}
function simulateOnMatch(rows, policy){
  const ps = pressSeries(rows);
  let pnlHT=0, nHT=0, pnlFT=0, nFT=0;

  for (let i=0;i<rows.length;i++){
    const m = rows[i].minute||0;
    if (m===0) continue;
    if (m>=90) break;

    const gd = (rows[i].goals_home||0)-(rows[i].goals_away||0);
    const p0 = lookupBaselineGlobal(m, gd); // baseline para uplift (janela base)

    // “probabilidade atual” via modelo (usamos baseline como proxy + gates de pressão)
    // Se você quiser, pode trocar aqui para sua prob misturada do modelo_counts.
    // Para simplificar treino, usamos pW ~= p0 mas gates de pressão decidem os disparos.
    const pW = p0; // proxy

    // projeções com hazard
    const pHT = (m<45) ? project(pW, BASE_W, 45-m) : 0;
    const pFT = project(pW, BASE_W, 90-m);

    // features de pressão
    const { domNow, ramp } = rampMetric(ps, policy.ramp_lb, m);
    const recentGoal = lastGoalWithin(rows, m, policy.cooldown);

    // gates comuns
    const passDom  = Math.abs(domNow) >= policy.min_dom;
    const passRamp = ramp >= policy.min_ramp;
    const passCD   = !recentGoal;

    // uplift vs baseline (usar projeções também é válido; aqui usamos pW/p0)
    const uplift = p0>0 ? (pW/p0) : 0;

    // -------- HT --------
    if (m<45 && passDom && passRamp && passCD) {
      if (pHT>=policy.ht.min_p && uplift>=policy.ht.min_uplift) {
        nHT++;
        const win = goalBetween(rows, m, 45);
        pnlHT += win ? WIN_HT : LOSE_ANY;
      }
    }

    // -------- FT --------
    if (passDom && passRamp && passCD) {
      if (pFT>=policy.ft.min_p && uplift>=policy.ft.min_uplift) {
        nFT++;
        const win = goalBetween(rows, m, 90);
        if (win) pnlFT += (m>=50 ? WIN_FT_AFTER50 : WIN_FT_BEFORE50);
        else pnlFT += LOSE_ANY;
      }
    }
  }

  return { signalsHT:nHT, pnlHT, signalsFT:nFT, pnlFT };
}

// gera todas combinações do grid e escolhe melhor por PnL
function* combos(){
  for (const min_p of GRID.min_p)
  for (const min_uplift of GRID.uplift)
  for (const min_dom of GRID.dom)
  for (const min_ramp of GRID.ramp)
  for (const cooldown of GRID.cooldown) {
    yield {
      min_dom, min_ramp, cooldown,
      ramp_lb: Number(process.env.SIGNAL_RAMP_LOOKBACK || 6),
      ht:{ min_p, min_uplift },
      ft:{ min_p, min_uplift },
    };
  }
}

function bestPolicyForScope(filterFn){
  const events = qEvents().all().map(r=>r.event_id);
  let best=null, bestPnl=-Infinity, bestCounts={ht:0, ft:0};

  for (const pol of combos()){
    let pnl=0, cHT=0, cFT=0, used=0;
    for (const eid of events){
      const meta = qMeta().get(eid) || {};
      if (!filterFn(meta)) continue;
      const rows = qTicks().all(eid);
      if (!rows.length) continue;
      const r = simulateOnMatch(rows, pol);
      pnl += (r.pnlHT + r.pnlFT);
      cHT += r.signalsHT;
      cFT += r.signalsFT;
      used++;
    }
    if (used===0) continue;
    if (pnl>bestPnl){
      bestPnl=pnl; best=pol; bestCounts={ht:cHT, ft:cFT};
    }
  }
  return { policy: best, pnl: bestPnl, counts: bestCounts };
}

function trainAndSave(){
  db.init();

  // GLOBAL
  const g = bestPolicyForScope(()=>true);
  if ( (g.counts.ht+g.counts.ft) >= MIN_SIGNALS_GLOBAL && g.policy){
    db.upsertPolicy({ scope_type:'global', scope_key:'*', params:g.policy });
    console.log('[train] saved GLOBAL', g.counts, 'PnL=', g.pnl.toFixed(2));
  } else {
    console.log('[train] global insufficient samples', g.counts);
  }

  // POR LIGA
  const qLeagues = ensureRaw().prepare(`SELECT DISTINCT league FROM matches`).all().map(r=>r.league||'');
  for (const L of qLeagues){
    const leagueKey = normalizeLeagueName(L);
    const r = bestPolicyForScope(meta => normalizeLeagueName(meta.league||'')===leagueKey);
    const total = (r.counts.ht+r.counts.ft);
    if (total >= MIN_SIGNALS_LEAGUE && r.policy){
      db.upsertPolicy({ scope_type:'league', scope_key:leagueKey, params:r.policy });
      console.log('[train] saved LEAGUE', leagueKey, r.counts, 'PnL=', r.pnl.toFixed(2));
    }
  }

  // POR TIME
  const qTeams = ensureRaw().prepare(`
    SELECT name FROM (
      SELECT DISTINCT home AS name FROM matches
      UNION
      SELECT DISTINCT away AS name FROM matches
    ) WHERE name IS NOT NULL AND name!=''
  `).all().map(r=>r.name);
  for (const T of qTeams){
    const teamKey = normalizeTeamName(T);
    const r = bestPolicyForScope(meta => (
      normalizeTeamName(meta.home||'')===teamKey ||
      normalizeTeamName(meta.away||'')===teamKey
    ));
    const total = (r.counts.ht+r.counts.ft);
    if (total >= MIN_SIGNALS_TEAM && r.policy){
      db.upsertPolicy({ scope_type:'team', scope_key:teamKey, params:r.policy });
      console.log('[train] saved TEAM', teamKey, r.counts, 'PnL=', r.pnl.toFixed(2));
    }
  }

  console.log('[train] done.');
}

if (require.main === module) trainAndSave();
