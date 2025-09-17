// Normalização + features discretizadas a partir dos deltas dos últimos minutos

// === normalização de nomes ===
function normalizeLeagueName(raw) {
  const s = String(raw || '').trim();
  if (s.length <= 8) return s;
  const cut = s.slice(0, -8).trim().replace(/[-–—:_|\s]+$/g, '').trim();
  return cut || s;
}
function normalizeTeamName(raw) { return String(raw || '').trim(); }

// === bins auxiliares ===
function clamp(v, a, b) { return Math.max(a, Math.min(b, v)); }

// contagens (finalizações, bloqueios, escanteios etc.): 0,1,2,3+
function binCountDelta(x) {
  const v = Math.max(0, Math.floor(+x || 0));
  if (v <= 0) return 0;
  if (v === 1) return 1;
  if (v === 2) return 2;
  return 3; // 3 ou mais
}

// xG/xGOT: faixas pequenas em décimos
// 0, 0.01–0.04, 0.05–0.09, 0.10–0.19, 0.20–0.34, 0.35+
function binXgDelta(x) {
  const v = Math.max(0, +x || 0);
  if (v < 0.005) return 0;
  if (v < 0.05)  return 1;
  if (v < 0.10)  return 2;
  if (v < 0.20)  return 3;
  if (v < 0.35)  return 4;
  return 5;
}

// minuto em buckets de 5
function binMinuteBucket(m) {
  const mm = Math.max(0, Math.floor(+m || 0));
  return clamp(Math.floor(mm / 5), 0, 20); // até 100'
}
// saldo de gols em [-2..2]
function binGoalDiff(gd) {
  const v = Math.floor(+gd || 0);
  return clamp(v, -2, 2);
}

// === lista de estatísticas que tentaremos usar ===
// Nome lógico  -> pares de colunas esperadas nos ticks (home/away)
const STAT_COLS = [
  { key:'xg',        home:'xg_home',        away:'xg_away',        type:'xg'    },
  { key:'xgot',      home:'xgot_home',      away:'xgot_away',      type:'xg'    },
  { key:'cc',        home:'cc_home',        away:'cc_away',        type:'cnt'   }, // chances claras
  { key:'st',        home:'st_home',        away:'st_away',        type:'cnt'   }, // total de finalizações
  { key:'sot',       home:'sot_home',       away:'sot_away',       type:'cnt'   }, // no alvo
  { key:'soff',      home:'soff_home',      away:'soff_away',      type:'cnt'   }, // pra fora
  { key:'sblk',      home:'sblk_home',      away:'sblk_away',      type:'cnt'   }, // bloqueadas
  { key:'inbox',     home:'inbox_home',     away:'inbox_away',     type:'cnt'   }, // dentro da área
  { key:'outbox',    home:'outbox_home',    away:'outbox_away',    type:'cnt'   }, // fora da área
  { key:'woodwork',  home:'wood_home',      away:'wood_away',      type:'cnt'   }, // bolas na trave
  { key:'corners',   home:'corners_home',   away:'corners_away',   type:'cnt'   },
  { key:'tda',       home:'tda_home',       away:'tda_away',       type:'cnt'   }, // toques na área adv.
];

// === assinatura por deltas entre a primeira e a última amostra da janela ===
function buildFeatureSignatureFromRows(rows) {
  if (!rows || rows.length < 2) return '';
  const first = rows[0], last = rows[rows.length - 1];

  const parts = [];
  for (const def of STAT_COLS) {
    const h0 = first[def.home], h1 = last[def.home];
    const a0 = first[def.away], a1 = last[def.away];
    const haveH = typeof h0 === 'number' && typeof h1 === 'number';
    const haveA = typeof a0 === 'number' && typeof a1 === 'number';
    if (!haveH || !haveA) continue; // se o jogo não tem essa estatística, ignoramos

    const dH = (h1 || 0) - (h0 || 0);
    const dA = (a1 || 0) - (a0 || 0);
    const sum = Math.max(0, (dH || 0) + (dA || 0));   // intensidade
    const dom = (dH || 0) - (dA || 0);                // domínio do lado

    let bSum, bDom;
    if (def.type === 'xg') {
      bSum = binXgDelta(sum);
      // para domínio de xG, usamos bin em valor absoluto + sinal
      const absDom = binXgDelta(Math.abs(dom));
      // codificamos o sinal: negativo vira sufixo 'N', positivo 'P', zero 'Z'
      const sign = dom > 0 ? 'P' : (dom < 0 ? 'N' : 'Z');
      parts.push(`${def.key}:S${bSum},D${absDom}${sign}`);
    } else {
      bSum = binCountDelta(sum);
      const absDom = binCountDelta(Math.abs(dom));
      const sign = dom > 0 ? 'P' : (dom < 0 ? 'N' : 'Z');
      parts.push(`${def.key}:S${bSum},D${absDom}${sign}`);
    }
  }

  // ordena p/ estabilidade e junta
  parts.sort();
  return parts.join(';'); // ex.: "cc:S1,D1P;corners:S0,D0Z;sot:S2,D1P;xg:S1,D0Z"
}

// hash FNV-1a (32-bit) – simples e estável
function fnv1a32(str) {
  let h = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = (h + ((h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24))) >>> 0;
  }
  return h >>> 0;
}

module.exports = {
  normalizeLeagueName,
  normalizeTeamName,
  binMinuteBucket,
  binGoalDiff,
  buildFeatureSignatureFromRows,
  fnv1a32,
};
