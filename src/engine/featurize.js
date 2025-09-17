// src/engine/featurize.js
// Extrai features multi-escala e rampas a partir dos ticks do DB (1/3/6/10min)
// Usa “minuto de jogo” para baseline (não relógio).

const db = require('../db');

// pesos iguais aos do painel/collector para pressão
function pressureFromDelta(dh, da) {
  const ph = (dh.sot || 0) * 3 + (dh.soff || 0) * 1.5 + (dh.da || 0) * 0.5 + (dh.corners || 0) * 0.5;
  const pa = (da.sot || 0) * 3 + (da.soff || 0) * 0 + (da.da || 0) * 0.5 + (da.corners || 0) * 0.5;
  return { home: +ph.toFixed(2), away: +pa.toFixed(2) };
}

function findTickAtOrBeforeMinute(ticksAsc, target) {
  if (!ticksAsc?.length) return null;
  let ans = null;
  for (let i = ticksAsc.length - 1; i >= 0; i--) {
    const t = ticksAsc[i];
    if (Number.isFinite(t.minute) && t.minute <= target) { ans = t; break; }
  }
  return ans || ticksAsc[0] || null;
}

function deltas(curr, base) {
  const d = (k) => (curr[k] || 0) - ((base && base[k]) || 0);
  return {
    h: {
      st: d('st_home'), sot: d('sot_home'), soff: d('soff_home'),
      da: d('da_home'), corners: d('corners_home')
    },
    a: {
      st: d('st_away'), sot: d('sot_away'), soff: d('soff_away'),
      da: d('da_away'), corners: d('corners_away')
    }
  };
}

function safeDiv(a, b) { return b > 0 ? a / b : 0; }

function computeWindowFeatures(histAsc, currTick, winMin) {
  const mNow = currTick.minute ?? 0;
  const base = findTickAtOrBeforeMinute(histAsc, Math.max(0, mNow - winMin)) || histAsc[0];
  const { h, a } = deltas(currTick, base);
  const press = pressureFromDelta(
    { sot: h.sot, soff: h.soff, da: h.da, corners: h.corners },
    { sot: a.sot, soff: a.soff, da: a.da, corners: a.corners }
  );
  const dom = +(press.home - press.away).toFixed(2);
  const psum = +(press.home + press.away).toFixed(2);

  // contagens brutas úteis também
  return {
    press_home: press.home,
    press_away: press.away,
    dom,
    psum,
    // chutes
    sot_h: h.sot, sot_a: a.sot,
    st_h:  h.st,  st_a:  a.st,
    soff_h: h.soff, soff_a: a.soff,
    cor_h: h.corners, cor_a: a.corners,
    da_h: h.da, da_a: a.da,
    rate_sot: safeDiv((h.sot + a.sot), winMin),
    rate_st:  safeDiv((h.st + a.st), winMin),
  };
}

function lastGoalMinute(histAsc) {
  let last = null;
  for (let i = 1; i < histAsc.length; i++) {
    const p = histAsc[i - 1], c = histAsc[i];
    const gp = (p.goals_home || 0) + (p.goals_away || 0);
    const gc = (c.goals_home || 0) + (c.goals_away || 0);
    if (gc > gp) last = c.minute ?? last;
  }
  return last; // pode ser null
}

/**
 * Retorna:
 * - features numéricas multi-escala (1/3/6/10) + rampas (derivadas)
 * - métricas auxiliares para política (dominância6, slope, minDesdeUltimoGol)
 */
function featurizeForMinute(event_id, currTick, histAsc) {
  const WINS = [1, 3, 6, 10];
  const f = {};
  const perWin = {};
  for (const w of WINS) {
    perWin[w] = computeWindowFeatures(histAsc, currTick, w);
    for (const [k, v] of Object.entries(perWin[w])) f[`${k}_${w}`] = v;
  }

  // rampas simples (aprox. slope) usando diferença de dom/psum entre janelas
  const slope_dom_6 = (perWin[3].dom - perWin[10].dom) / (3 - 10);   // >0 = acelerando
  const slope_ps_6  = (perWin[3].psum - perWin[10].psum) / (3 - 10);

  const lgm = lastGoalMinute(histAsc);
  const minDesdeUltGol = Number.isFinite(lgm) ? Math.max(0, (currTick.minute || 0) - lgm) : 999;

  const gd = (currTick.goals_home || 0) - (currTick.goals_away || 0);
  const minute = currTick.minute || 0;
  const isFT = minute > 45;

  return {
    features: {
      minute,
      isFT: isFT ? 1 : 0,
      gd: Math.max(-2, Math.min(2, gd)),
      // dom/psum e taxas nas janelas
      dom_1: perWin[1].dom, dom_3: perWin[3].dom, dom_6: perWin[6].dom, dom_10: perWin[10].dom,
      psum_1: perWin[1].psum, psum_3: perWin[3].psum, psum_6: perWin[6].psum, psum_10: perWin[10].psum,
      rate_sot_1: perWin[1].rate_sot, rate_sot_3: perWin[3].rate_sot, rate_sot_6: perWin[6].rate_sot, rate_sot_10: perWin[10].rate_sot,
      rate_st_1: perWin[1].rate_st,   rate_st_3: perWin[3].rate_st,   rate_st_6: perWin[6].rate_st,   rate_st_10: perWin[10].rate_st,
      slope_dom_6, slope_ps_6,
      // contagens brutas recentes
      sot_h_6: perWin[6].sot_h, sot_a_6: perWin[6].sot_a,
      st_h_6:  perWin[6].st_h,  st_a_6:  perWin[6].st_a,
      cor_h_6: perWin[6].cor_h, cor_a_6: perWin[6].cor_a,
      da_h_6:  perWin[6].da_h,  da_a_6:  perWin[6].da_a,
    },
    aux: {
      dom6: perWin[6].dom,
      slope6: slope_dom_6,
      psum6: perWin[6].psum,
      minDesdeUltGol
    }
  };
}

module.exports = { featurizeForMinute };
