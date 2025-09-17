// src/engine/ml_infer.js
// Predição de P(gol até o fim do tempo ATUAL): retorna { p_ht, p_ft }.
// 1) tenta microserviço Python (opcional) em ML_URL
// 2) fallback: heurística calibrada sobre features (funciona já)

const fetch = (...args) => import('node-fetch').then(({default: f}) => f(...args));

const ML_URL = process.env.ML_URL || 'http://127.0.0.1:5005/predict';

// regressão logística “manual” só pra ter um fallback razoável
function sigmoid(x){ return 1/(1+Math.exp(-x)); }

// pesos chutados/ajustados empiricamente; ajuste depois com treino
function heuristicProb(features, half) {
  const m = features.minute || 0;
  const dom = features.dom_6 || 0;
  const slope = features.slope_dom_6 || 0;
  const ps = features.psum_6 || 0;
  const rateSOT = features.rate_sot_6 || 0;
  const rateST  = features.rate_st_6  || 0;
  const gd = features.gd || 0;

  // HT e FT têm interceptos diferentes
  const b0 = (half === 'HT') ? -1.25 : -1.1;
  const w = (
    0.045*dom + 0.09*slope + 0.03*ps + 0.35*rateSOT + 0.06*rateST
    - 0.06*Math.max(0, -gd)   // atrás no placar ↑ ataque mas também reduz tempo útil
    - 0.005*m                 // tempo correndo reduz tempo restante
  );
  const pMin = sigmoid(b0 + w);
  return Math.max(0.01, Math.min(0.95, pMin));
}

// se o Python estiver rodando, melhor usar o hazard/GBM calibrado
async function inferWithServer(payload) {
  try {
    const r = await fetch(ML_URL, {
      method: 'POST',
      headers: {'content-type':'application/json'},
      body: JSON.stringify(payload),
      timeout: 800
    });
    if (!r.ok) throw new Error(String(r.status));
    const j = await r.json();
    if (typeof j.p_ht === 'number' && typeof j.p_ft === 'number') return j;
  } catch {}
  return null;
}

async function predictGoalProbs(feats) {
  const payload = { features: feats };
  const online = await inferWithServer(payload);
  if (online) return online;

  // fallback heurístico
  const p_ht = heuristicProb(feats, 'HT');
  const p_ft = heuristicProb(feats, 'FT');
  return { p_ht, p_ft };
}

module.exports = { predictGoalProbs };
