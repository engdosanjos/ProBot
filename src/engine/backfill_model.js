// src/engine/backfill_model.js
require('dotenv').config();

const path = require('path');
const Better = require('better-sqlite3');

const {
  normalizeLeagueName,
  normalizeTeamName,
  binMinuteBucket,
  binGoalDiff,
  buildFeatureSignatureFromRows,
  fnv1a32,
} = require('./model_features');

const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', '..', 'data', 'events.db');

const WINDOWS_MIN  = [5, 10, 15, 20, 25];
const LOOKBACK_MIN = Number(process.env.PRESS_LOOKBACK_MIN || 6); // janela dos deltas (min)

// --- conexão + schema (somente leitura p/ queries e escrita p/ contadores) ---
let raw;
function ensureRaw() {
  if (!raw) {
    raw = new Better(DB_PATH);
    // Em better-sqlite3, quando usamos "=" no PRAGMA, precisamos do { simple: true }
    raw.pragma('journal_mode = WAL', { simple: true });
    raw.pragma('busy_timeout = 5000', { simple: true });
    ensureModelSchema(raw);
    prepareWriters(raw);
  }
  return raw;
}

function ensureModelSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS model_counts (
      scope_type   TEXT,                   -- 'global' | 'league' | 'team'
      scope_key    TEXT,                   -- '*' | nome da liga | nome do time
      window       INTEGER,                -- 5|10|15|20|25|...
      side         TEXT,                   -- 'home' | 'away' | 'any'
      pdom_bin     INTEGER,                -- ASSINATURA hash (fnv1a32) do padrão dos deltas
      psum_bin     INTEGER,                -- mantido por compat (usamos 0)
      m_bucket     INTEGER,                -- minuto // 5
      gd_bin       INTEGER,                -- clamp(gd,-2..2)
      n            INTEGER DEFAULT 0,      -- amostras
      y            INTEGER DEFAULT 0,      -- acertos (teve gol na janela)
      PRIMARY KEY (scope_type, scope_key, window, side, pdom_bin, psum_bin, m_bucket, gd_bin)
    );
  `);
}

// writers preparados (upsert de contadores)
let stmtBumpModel;
function prepareWriters(db) {
  stmtBumpModel = db.prepare(`
    INSERT INTO model_counts (
      scope_type, scope_key, window, side,
      pdom_bin, psum_bin, m_bucket, gd_bin, n, y
    )
    VALUES (@scope_type, @scope_key, @window, @side,
            @pdom_bin, @psum_bin, @m_bucket, @gd_bin, @n, @y)
    ON CONFLICT(scope_type, scope_key, window, side, pdom_bin, psum_bin, m_bucket, gd_bin)
    DO UPDATE SET
      n = model_counts.n + excluded.n,
      y = model_counts.y + excluded.y
  `);
}

// --- consultas de leitura ---
const qDistinctEvents = () => ensureRaw().prepare(`
  SELECT DISTINCT event_id FROM ticks
  WHERE minute IS NOT NULL
`);
const qTicksByEvent = () => ensureRaw().prepare(`
  SELECT *
  FROM ticks
  WHERE event_id = ?
    AND minute IS NOT NULL
  ORDER BY minute ASC, ts ASC
`);
const qMatchMeta = () => ensureRaw().prepare(`
  SELECT league, home, away FROM matches WHERE event_id = ?
`);

// ---------- helpers ----------
/**
 * Retorna o índice do primeiro tick dentro da janela de LOOKBACK_MIN minutos
 * anterior ao tick i (inclusive). Usa busca binária por 'minute'.
 */
function findBaselineTickIndex(rows, i, lookbackMin) {
  const mNow = Math.max(0, rows[i].minute || 0);
  const target = Math.max(0, mNow - lookbackMin);

  // busca binária no intervalo [0..i]
  let lo = 0, hi = i, ans = i;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const m = Math.max(0, rows[mid].minute || 0);
    if (m >= target) {
      ans = mid;
      hi = mid - 1;
    } else {
      lo = mid + 1;
    }
  }
  return ans;
}

/**
 * Verifica se houve pelo menos 1 gol entre (minute_i, minute_i + W], olhando soma de gols.
 */
function goalWithinWindow(rows, i, W) {
  const mStart = Math.max(0, rows[i].minute || 0);
  const mEnd = mStart + W;
  const goalsStart = (rows[i].goals_home || 0) + (rows[i].goals_away || 0);

  for (let k = i + 1; k < rows.length; k++) {
    const mk = Math.max(0, rows[k].minute || 0);
    if (mk > mEnd) break;
    const gk = (rows[k].goals_home || 0) + (rows[k].goals_away || 0);
    if (gk > goalsStart) return true;
  }
  return false;
}

/**
 * Upsert dos contadores do modelo nos três escopos.
 */
function bumpAllScopes({ leagueKey, homeKey, awayKey }, windowMin, featHash, minuteBucket, gdBin, y) {
  const common = {
    window: windowMin,
    side: 'any',
    pdom_bin: featHash, // assinatura-hash
    psum_bin: 0,
    m_bucket: minuteBucket,
    gd_bin: gdBin,
    n: 1,
    y: y ? 1 : 0,
  };

  stmtBumpModel.run({ scope_type: 'global', scope_key: '*', ...common });
  stmtBumpModel.run({ scope_type: 'league', scope_key: leagueKey, ...common });
  stmtBumpModel.run({ scope_type: 'team',   scope_key: homeKey,   ...common });
  stmtBumpModel.run({ scope_type: 'team',   scope_key: awayKey,   ...common });
}

// ---------- backfill para um evento ----------
const backfillEventTx = () => raw.transaction((event_id) => {
  const rows = qTicksByEvent().all(event_id);
  if (!rows || rows.length < 3) return { nSamples: 0, nPos: 0 };

  const meta = qMatchMeta().get(event_id) || {};
  const leagueKey = normalizeLeagueName(meta.league || '');
  const homeKey   = normalizeTeamName(meta.home || '');
  const awayKey   = normalizeTeamName(meta.away || '');

  let nSamples = 0;
  let nPos = 0;

  for (let i = 0; i < rows.length; i++) {
    const mi = rows[i].minute;
    if (!Number.isFinite(mi)) continue;

    // achar início da janela de lookback
    const j = findBaselineTickIndex(rows, i, LOOKBACK_MIN);
    if (j === null || j === undefined) continue;

    // recorte da janela [j..i] para montar a assinatura
    const win = rows.slice(j, i + 1);

    let signature = buildFeatureSignatureFromRows(win);
    if (!signature) signature = 'NONE';
    let featHash = fnv1a32(signature);
    if (featHash === 0) featHash = 1; // 0 reservado para wildcard

    const minuteBucket = binMinuteBucket(mi);
    const gdBin = binGoalDiff((rows[i].goals_home || 0) - (rows[i].goals_away || 0));

    // para cada janela de previsão, verifica se houve gol e atualiza contadores
    for (const W of WINDOWS_MIN) {
      const y = goalWithinWindow(rows, i, W) ? 1 : 0;
      bumpAllScopes({ leagueKey, homeKey, awayKey }, W, featHash, minuteBucket, gdBin, y);
      nSamples++;
      nPos += y;
    }
  }

  return { nSamples, nPos };
});

function backfillOne(event_id) {
  return backfillEventTx()(event_id);
}

// ---------- main ----------
function main() {
  const events = qDistinctEvents().all().map(r => r.event_id);
  console.log(`[backfill] eventos com ticks: ${events.length}`);

  let totalSamples = 0;
  let totalPos = 0;

  events.forEach((eid, idx) => {
    const { nSamples, nPos } = backfillOne(eid);
    totalSamples += nSamples;
    totalPos += nPos;
    if ((idx + 1) % 25 === 0 || idx === events.length - 1) {
      const acc = totalSamples ? (totalPos / totalSamples) * 100 : 0;
      console.log(`[backfill] ${idx + 1}/${events.length}  amostras+=${nSamples} pos+=${nPos}  (acc parcial ${acc.toFixed(2)}%)`);
    }
  });

  const base = totalSamples ? (totalPos / totalSamples) * 100 : 0;
  console.log(`[backfill] concluído. samples=${totalSamples} positives=${totalPos}  base_rate=${base.toFixed(2)}%`);
}

if (require.main === module) {
  ensureRaw(); // garante schema e statements
  main();
}

module.exports = { main };
