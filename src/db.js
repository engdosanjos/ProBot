// src/db.js
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', 'data', 'events.db');

let db;
let stmts = {};

function ensureDir(p) { try { fs.mkdirSync(p, { recursive: true }); } catch {} }

function pragma(db) {
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('temp_store = MEMORY');
  db.pragma('cache_size = -50000'); // ~50MB
  // ajuda em concorrência (better-sqlite3 aceita string com '=')
  db.pragma('busy_timeout = 5000');
}

function columnExists(tbl, col) {
  const rows = db.prepare(`PRAGMA table_info(${tbl})`).all();
  return rows.some(r => r.name === col);
}

function ensureSchema() {
  db.exec(`
    CREATE TABLE IF NOT EXISTS matches (
      rowid      INTEGER PRIMARY KEY,
      event_id   TEXT UNIQUE,
      url        TEXT,
      league     TEXT,
      home       TEXT,
      away       TEXT,
      created_at INTEGER,
      closed_at  INTEGER
    );

    CREATE TABLE IF NOT EXISTS ticks (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id    TEXT,
      ts          INTEGER,
      minute      INTEGER,
      status      TEXT,
      goals_home  INTEGER,
      goals_away  INTEGER,
      st_home     INTEGER,
      st_away     INTEGER,
      sot_home    INTEGER,
      sot_away    INTEGER,
      soff_home   INTEGER,
      soff_away   INTEGER,
      da_home     INTEGER,
      da_away     INTEGER,
      corners_home INTEGER,
      corners_away INTEGER
    );

    -- Previsões que o painel lê
    CREATE TABLE IF NOT EXISTS predictions (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id    TEXT,
      ts          INTEGER,
      league      TEXT,
      home        TEXT,
      away        TEXT,
      minute      INTEGER,
      window_min  INTEGER,
      prob        REAL
    );

    -- Modelo online (contagens por bins/assinaturas)
    CREATE TABLE IF NOT EXISTS model_counts (
      scope_type   TEXT,                   -- 'global' | 'league' | 'team'
      scope_key    TEXT,                   -- '*' | nome da liga | nome do time
      window       INTEGER,                -- 5|10|15|20|25|30|45|90...
      side         TEXT,                   -- 'home' | 'away' | 'any'
      pdom_bin     INTEGER,                -- assinatura/hash do padrão (fnv1a32)
      psum_bin     INTEGER,                -- mantido por compat (0)
      m_bucket     INTEGER,                -- minuto // 5
      gd_bin       INTEGER,                -- clamp(gd,-2..2)
      n            INTEGER DEFAULT 0,      -- amostras
      y            INTEGER DEFAULT 0,      -- acertos (teve gol na janela)
      PRIMARY KEY (scope_type, scope_key, window, side, pdom_bin, psum_bin, m_bucket, gd_bin)
    );

    -- Pendências (janelas abertas aguardando gol ou expirarem)
    CREATE TABLE IF NOT EXISTS pending_windows (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id      TEXT,
      league        TEXT,
      home          TEXT,
      away          TEXT,
      created_ts    INTEGER,
      minute        INTEGER,
      window        INTEGER,
      side          TEXT,                 -- 'home'|'away'|'any'
      pdom_bin      INTEGER,
      psum_bin      INTEGER,
      m_bucket      INTEGER,
      gd_bin        INTEGER,
      expire_minute INTEGER,              -- m+window (ou 45/90 para HT/FT)
      settled       INTEGER DEFAULT 0,
      won           INTEGER,              -- 1|0
      settle_minute INTEGER,
      scorer_side   TEXT                  -- 'home'|'away'|'any'|NULL
      -- coluna 'sig' será adicionada via migration se faltar
    );

    -- Sinais/Entradas emitidos pela predição
    CREATE TABLE IF NOT EXISTS signals (
      id            INTEGER PRIMARY KEY AUTOINCREMENT,
      event_id      TEXT,
      league        TEXT,
      home          TEXT,
      away          TEXT,
      created_ts    INTEGER,
      minute        INTEGER,
      window        INTEGER,
      side          TEXT,                 -- 'home'|'away'|'any'|'HT'|'FT'
      prob          REAL,
      p_global      REAL,
      p_league      REAL,
      p_team        REAL,
      status        TEXT DEFAULT 'open',  -- 'open'|'won'|'lost' (painel mapeia green/red)
      expire_minute INTEGER,
      settle_minute INTEGER,
      result        TEXT,                 -- 'green'|'red'|'void'
      pnl           REAL                  -- +0.1 | -1.0 | 0
    );
  `);

  // migrações simples
  if (!columnExists('matches', 'closed_at')) {
    db.exec(`ALTER TABLE matches ADD COLUMN closed_at INTEGER;`);
  }
  // >>> adiciona coluna opcional 'sig' em pending_windows, se não existir
  if (!columnExists('pending_windows', 'sig')) {
    db.exec(`ALTER TABLE pending_windows ADD COLUMN sig TEXT;`);
  }
}

function ensureIndexes() {
  db.exec(`
    -- ticks
    CREATE INDEX IF NOT EXISTS idx_ticks_event_ts      ON ticks(event_id, ts);
    CREATE INDEX IF NOT EXISTS idx_ticks_event_minute  ON ticks(event_id, minute);
    CREATE INDEX IF NOT EXISTS idx_ticks_ts            ON ticks(ts);
    CREATE INDEX IF NOT EXISTS idx_matches_league      ON matches(league);
    CREATE INDEX IF NOT EXISTS idx_matches_home        ON matches(home);
    CREATE INDEX IF NOT EXISTS idx_matches_away        ON matches(away);

    -- predictions
    CREATE INDEX IF NOT EXISTS idx_predictions_event_ts  ON predictions(event_id, ts);
    CREATE INDEX IF NOT EXISTS idx_predictions_event_win ON predictions(event_id, window_min, ts);

    -- modelo
    CREATE INDEX IF NOT EXISTS idx_model_scope_win ON model_counts(scope_type, scope_key, window, side);

    -- pendências
    CREATE INDEX IF NOT EXISTS idx_pending_event   ON pending_windows(event_id);
    CREATE INDEX IF NOT EXISTS idx_pending_expire  ON pending_windows(expire_minute);
    CREATE INDEX IF NOT EXISTS idx_pending_open    ON pending_windows(event_id, settled, expire_minute);

    -- sinais
    CREATE INDEX IF NOT EXISTS idx_signals_event   ON signals(event_id);
    CREATE INDEX IF NOT EXISTS idx_signals_status  ON signals(status);
    CREATE INDEX IF NOT EXISTS idx_signals_expire  ON signals(expire_minute);
  `);
}

function prepareStatements() {
  // matches
  stmts.upsertMatch = db.prepare(`
    INSERT INTO matches (event_id, url, league, home, away, created_at)
    VALUES (@event_id, @url, @league, @home, @away, @created_at)
    ON CONFLICT(event_id) DO UPDATE SET
      url    = excluded.url,
      league = excluded.league,
      home   = excluded.home,
      away   = excluded.away
  `);
  stmts.closeMatch = db.prepare(`UPDATE matches SET closed_at = @closed_at WHERE event_id = @event_id`);

  // ticks
  stmts.insertTick = db.prepare(`
    INSERT INTO ticks (
      event_id, ts, minute, status,
      goals_home, goals_away,
      st_home, st_away, sot_home, sot_away,
      soff_home, soff_away, da_home, da_away,
      corners_home, corners_away
    ) VALUES (
      @event_id, @ts, @minute, @status,
      @goals_home, @goals_away,
      @st_home, @st_away, @sot_home, @sot_away,
      @soff_home, @soff_away, @da_home, @da_away,
      @corners_home, @corners_away
    )
  `);
  stmts.insertTickBatch = db.transaction(rows => { for (const r of rows) stmts.insertTick.run(r); });

  // predictions
  stmts.insertPrediction = db.prepare(`
    INSERT INTO predictions (event_id, ts, league, home, away, minute, window_min, prob)
    VALUES (@event_id, @ts, @league, @home, @away, @minute, @window_min, @prob)
  `);

  // modelo
  stmts.bumpModel = db.prepare(`
    INSERT INTO model_counts (
      scope_type, scope_key, window, side,
      pdom_bin, psum_bin, m_bucket, gd_bin, n, y
    )
    VALUES (@scope_type, @scope_key, @window, @side, @pdom_bin, @psum_bin, @m_bucket, @gd_bin, @n, @y)
    ON CONFLICT(scope_type, scope_key, window, side, pdom_bin, psum_bin, m_bucket, gd_bin)
    DO UPDATE SET n = n + excluded.n, y = y + excluded.y
  `);

  // pendências — agora com 'sig' opcional
  stmts.addPending = db.prepare(`
    INSERT INTO pending_windows (
      event_id, league, home, away, created_ts, minute, window, side,
      pdom_bin, psum_bin, m_bucket, gd_bin, expire_minute, sig
    ) VALUES (
      @event_id, @league, @home, @away, @created_ts, @minute, @window, @side,
      @pdom_bin, @psum_bin, @m_bucket, @gd_bin, @expire_minute, @sig
    )
  `);
  stmts.getOpenPendings = db.prepare(`SELECT * FROM pending_windows WHERE event_id = ? AND settled = 0`);
  stmts.settlePending = db.prepare(`
    UPDATE pending_windows
       SET settled = 1, won = @won, settle_minute = @settle_minute, scorer_side = @scorer_side
     WHERE id = @id
  `);

  // sinais
  stmts.insertSignal = db.prepare(`
    INSERT INTO signals (
      event_id, league, home, away, created_ts, minute, window, side,
      prob, p_global, p_league, p_team, status, expire_minute
    ) VALUES (
      @event_id, @league, @home, @away, @created_ts, @minute, @window, @side,
      @prob, @p_global, @p_league, @p_team, 'open', @expire_minute
    )
  `);
  stmts.getOpenSignals = db.prepare(`SELECT * FROM signals WHERE status = 'open' AND event_id = ?`);
  stmts.settleSignal  = db.prepare(`
    UPDATE signals
       SET status = @status, settle_minute = @settle_minute, result = @result, pnl = @pnl
     WHERE id = @id
  `);

  // consultas úteis
  stmts.getTicksRange = db.prepare(`SELECT * FROM ticks WHERE event_id = ? AND ts BETWEEN ? AND ? ORDER BY ts`);
  stmts.getLastTick   = db.prepare(`SELECT * FROM ticks WHERE event_id = ? ORDER BY ts DESC LIMIT 1`);
  stmts.getModelRow   = db.prepare(`
    SELECT n, y FROM model_counts
    WHERE scope_type = ? AND scope_key = ? AND window = ? AND side = ?
      AND pdom_bin = ? AND psum_bin = ? AND m_bucket = ? AND gd_bin = ?
  `);
}

function init() {
  ensureDir(path.dirname(DB_PATH));
  db = new Database(DB_PATH);
  pragma(db);
  ensureSchema();
  ensureIndexes();
  prepareStatements();
  setInterval(() => { try { db.exec('PRAGMA optimize; ANALYZE;'); } catch {} }, 60 * 60 * 1000);
}

// API base
function upsertMatch(obj) { stmts.upsertMatch.run({ ...obj, created_at: obj.created_at || Date.now() }); }
function closeMatch(event_id) { stmts.closeMatch.run({ event_id, closed_at: Date.now() }); }
function insertTick(row) { stmts.insertTick.run(row); }
function insertTickBatch(rows) { if (rows?.length) stmts.insertTickBatch(rows); }
function insertPrediction(row) { stmts.insertPrediction.run(row); }

function getTicksRange(event_id, fromTs, toTs) { return stmts.getTicksRange.all(event_id, fromTs, toTs); }
function getLastTick(event_id) { return stmts.getLastTick.get(event_id); }

// modelo/pendências/sinais
function bumpModel(r) { stmts.bumpModel.run(r); }
function addPending(r) { stmts.addPending.run(r); }
function getOpenPendings(event_id) { return stmts.getOpenPendings.all(event_id); }
function settlePending(r) { stmts.settlePending.run(r); }

function insertSignal(r) { stmts.insertSignal.run(r); }
function getOpenSignals(event_id) { return stmts.getOpenSignals.all(event_id); }
function settleSignal(r) { stmts.settleSignal.run(r); }

function getModelRow(...args) { return stmts.getModelRow.get(...args); }

module.exports = {
  init, path: DB_PATH,
  upsertMatch, closeMatch, insertTick, insertTickBatch, insertPrediction,
  getTicksRange, getLastTick,
  bumpModel, addPending, getOpenPendings, settlePending,
  insertSignal, getOpenSignals, settleSignal, getModelRow
};
