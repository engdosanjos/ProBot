// src/panel/server.js
require('dotenv').config();
const express = require('express');
const path = require('path');
const Better = require('better-sqlite3');

const DB_PATH = process.env.DB_PATH || path.join(__dirname, '..', '..', 'data', 'events.db');
const PORT    = Number(process.env.PANEL_PORT || 3000);
const LIVE_WINDOW_MS = Number(process.env.LIVE_WINDOW_MS || 3 * 60 * 1000); // últimos 3 min
const RECENT_SIGNALS = Number(process.env.RECENT_SIGNALS || 10);
// >>> alinhado com o analisador (default 6 min)
const PRESS_WINDOW_MIN = Math.max(1, Number(process.env.PRESS_WINDOW_MIN || 6));

const db = new Better(DB_PATH);
db.pragma('journal_mode = WAL', { simple: true });
db.pragma('busy_timeout = 5000', { simple: true });

function tableExists(name) {
  const row = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?").get(name);
  return !!row;
}
function tableHas(name, cols) {
  try {
    const info = db.prepare(`PRAGMA table_info(${name})`).all();
    const names = new Set(info.map(r => r.name));
    return cols.every(c => names.has(c));
  } catch { return false; }
}

// ——— Tabelas auxiliares (se não existirem) ———
db.exec(`
CREATE TABLE IF NOT EXISTS predictions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id   TEXT,
  ts         INTEGER,
  league     TEXT,
  home       TEXT,
  away       TEXT,
  minute     INTEGER,
  window_min INTEGER,
  prob       REAL
);
CREATE INDEX IF NOT EXISTS idx_predictions_event_ts  ON predictions(event_id, ts);
CREATE INDEX IF NOT EXISTS idx_predictions_event_win ON predictions(event_id, window_min, ts);

/* SÓ cria se não existirem — compatível com bases legadas. */
CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id   TEXT,
  created_at INTEGER,
  closed_at  INTEGER,
  status     TEXT,     -- 'open' | 'green' | 'red' | 'won' | 'lost'
  side       TEXT,     -- 'HT' | 'FT' | 'home' | 'away' | 'any'
  window_min INTEGER,  -- alguns bancos legados usam 'window'
  prob       REAL,
  pnl        REAL,
  league     TEXT,
  home       TEXT,
  away       TEXT
);
`);

// ——— Índices dinâmicos para signals ———
if (tableExists('signals')) {
  const hasCreatedAt = tableHas('signals', ['created_at']);
  const hasCreatedTs = tableHas('signals', ['created_ts']);
  try {
    if (hasCreatedAt) {
      db.exec(`CREATE INDEX IF NOT EXISTS idx_signals_status_created_at ON signals(status, created_at);`);
    } else if (hasCreatedTs) {
      db.exec(`CREATE INDEX IF NOT EXISTS idx_signals_status_created_ts ON signals(status, created_ts);`);
    } else {
      db.exec(`CREATE INDEX IF NOT EXISTS idx_signals_status_only       ON signals(status);`);
    }
  } catch {}
}

// ——— Prepared: último tick de cada jogo “vivo” ———
// (inclui colunas novas: sblk/bc/xg)
const stmtLiveRows = db.prepare(`
  SELECT
    t.event_id, t.ts, t.minute, t.status,
    t.goals_home, t.goals_away,
    t.st_home, t.st_away, t.sot_home, t.sot_away,
    t.soff_home, t.soff_away, t.da_home, t.da_away,
    t.corners_home, t.corners_away,
    t.sblk_home, t.sblk_away, t.bc_home, t.bc_away,
    t.xg_home, t.xg_away,
    m.league, m.home, m.away, m.url
  FROM ticks t
  JOIN (
    SELECT event_id, MAX(ts) AS ts
    FROM ticks
    WHERE ts > @cutoff
    GROUP BY event_id
  ) last ON last.event_id = t.event_id AND last.ts = t.ts
  LEFT JOIN matches m ON m.event_id = t.event_id
  ORDER BY t.ts DESC
`);

// (fallback ocasional)
const stmtPrevTick = db.prepare(`
  SELECT *
  FROM ticks
  WHERE event_id = ? AND ts < ?
  ORDER BY ts DESC
  LIMIT 1
`);

// ——— Prepared: baseline por “minuto de jogo” (alinhado ao analisador) ———
const stmtBaselineByMinute = db.prepare(`
  SELECT *
  FROM ticks
  WHERE event_id = ? AND minute IS NOT NULL AND minute <= ?
  ORDER BY minute DESC, ts DESC
  LIMIT 1
`);

// ——— Prepared: previsões mais recentes por (jogo, janela), com corte de “vivo” ———
const stmtLatestPredictions = db.prepare(`
  SELECT p.*
  FROM predictions p
  JOIN (
    SELECT event_id, window_min, MAX(ts) AS ts
    FROM predictions
    WHERE ts > @cutoff
    GROUP BY event_id, window_min
  ) last
    ON last.event_id = p.event_id
   AND last.window_min = p.window_min
   AND last.ts = p.ts
  WHERE p.event_id IN (
    SELECT event_id FROM (SELECT DISTINCT event_id FROM ticks WHERE ts > @cutoff)
  )
`);

// ——— Prepared: signals (ORDER BY dinâmico) ———
let stmtOpenSignals = null;
let stmtRecentSignals = null;
let stmtAggSignals = null;

if (tableExists('signals')) {
  const hasCreatedAt = tableHas('signals', ['created_at']);
  const hasCreatedTs = tableHas('signals', ['created_ts']);
  const hasClosedAt  = tableHas('signals', ['closed_at']);

  const orderCreated = hasCreatedAt ? 'created_at'
                     : hasCreatedTs ? 'created_ts'
                     : 'rowid';

  const orderClosed  = hasClosedAt ? 'closed_at' : orderCreated;

  stmtOpenSignals = db.prepare(`
    SELECT *
    FROM signals
    WHERE status='open'
    ORDER BY ${orderCreated} DESC
  `);

  stmtRecentSignals = db.prepare(`
    SELECT *
    FROM signals
    WHERE status IN ('green','red','won','lost')
    ORDER BY ${orderClosed} DESC
    LIMIT @n
  `);

  stmtAggSignals = db.prepare(`
    SELECT
      SUM(CASE WHEN status IN ('green','won') THEN 1 ELSE 0 END) AS greens,
      SUM(CASE WHEN status IN ('red','lost')  THEN 1 ELSE 0 END) AS reds,
      SUM(CASE WHEN status = 'open'          THEN 1 ELSE 0 END) AS open_cnt,
      SUM(COALESCE(pnl,0)) AS pnl
    FROM signals
  `);
}

const app = express();

// ——— utils ———
function pressureFromDelta(dh, da) {
  const ph = (dh.sot || 0) * 3 + (dh.soff || 0) * 1.5 + (dh.da || 0) * 0.5 + (dh.corners || 0) * 0.5;
  const pa = (da.sot || 0) * 3 + (da.soff || 0) * 1.5 + (da.da || 0) * 0.5 + (da.corners || 0) * 0.5;
  return { home: +ph.toFixed(2), away: +pa.toFixed(2) };
}

// ——— rotas ———
app.get('/api/live', (req, res) => {
  try {
    const cutoff = Date.now() - LIVE_WINDOW_MS;

    // previsões “vivas”
    const preds = stmtLatestPredictions.all({ cutoff });
    const predsByEvent = new Map();
    for (const p of preds) {
      const arr = predsByEvent.get(p.event_id) || [];
      arr.push({ window_min: p.window_min, prob: p.prob });
      predsByEvent.set(p.event_id, arr);
    }

    // último tick “vivo” por jogo
    const lastRows = stmtLiveRows.all({ cutoff });
    const out = [];

    for (const r of lastRows) {
      // baseline por minuto de jogo (minuteNow - PRESS_WINDOW_MIN)
      const minuteNow = Number.isFinite(r.minute) ? r.minute : 0;
      const targetMin = Math.max(0, (minuteNow || 0) - PRESS_WINDOW_MIN);

      // tenta baseline alinhado ao analisador; se não houver (início), cai pro tick anterior; se ainda não houver, usa o próprio
      const base = stmtBaselineByMinute.get(r.event_id, targetMin)
                || stmtPrevTick.get(r.event_id, r.ts)
                || r;

      // deltas de 6min para pressão (e para novas métricas também, útil no front)
      const deltaH = {
        sot: (r.sot_home || 0) - (base?.sot_home || 0),
        soff: (r.soff_home || 0) - (base?.soff_home || 0),
        da: (r.da_home || 0) - (base?.da_home || 0),
        corners: (r.corners_home || 0) - (base?.corners_home || 0),
        sblk: (r.sblk_home || 0) - (base?.sblk_home || 0),
        bc:   (r.bc_home   || 0) - (base?.bc_home   || 0),
        xg:   (r.xg_home   || 0) - (base?.xg_home   || 0),
      };
      const deltaA = {
        sot: (r.sot_away || 0) - (base?.sot_away || 0),
        soff: (r.soff_away || 0) - (base?.soff_away || 0),
        da: (r.da_away || 0) - (base?.da_away || 0),
        corners: (r.corners_away || 0) - (base?.corners_away || 0),
        sblk: (r.sblk_away || 0) - (base?.sblk_away || 0),
        bc:   (r.bc_away   || 0) - (base?.bc_away   || 0),
        xg:   (r.xg_away   || 0) - (base?.xg_away   || 0),
      };

      const press = pressureFromDelta(deltaH, deltaA);

      out.push({
        event_id: r.event_id,
        ts: r.ts,
        url: r.url,
        league: r.league,
        home: r.home, away: r.away,
        minute: r.minute, status: r.status,
        goals_home: r.goals_home, goals_away: r.goals_away,

        // stats “atuais”
        st_home: r.st_home, st_away: r.st_away,
        sot_home: r.sot_home, sot_away: r.sot_away,
        soff_home: r.soff_home, soff_away: r.soff_away,
        da_home: r.da_home, da_away: r.da_away,
        corners_home: r.corners_home, corners_away: r.corners_away,

        // novas métricas “atuais”
        sblk_home: r.sblk_home, sblk_away: r.sblk_away,
        bc_home:   r.bc_home,   bc_away:   r.bc_away,
        xg_home:   r.xg_home,   xg_away:   r.xg_away,

        // pressão (Δ6m ponderado)
        pressure: press,

        // deltas na janela (útil pro front/heat)
        window_delta: {
          sblk_home: deltaH.sblk, sblk_away: deltaA.sblk,
          bc_home:   deltaH.bc,   bc_away:   deltaA.bc,
          xg_home:   +(+deltaH.xg).toFixed(3), xg_away: +(+deltaA.xg).toFixed(3),
        },

        // previsões por janela
        windows: (predsByEvent.get(r.event_id) || []).sort((a,b)=>a.window_min-b.window_min)
      });
    }

    res.json({ ok: true, rows: out });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get('/api/snapshot', (req, res) => {
  try {
    const cutoff = Date.now() - LIVE_WINDOW_MS;
    const live = db.prepare(`
      SELECT COUNT(*) AS c FROM (
        SELECT event_id, MAX(ts) AS ts
        FROM ticks
        WHERE ts > @cutoff
        GROUP BY event_id
      )`).get({ cutoff })?.c || 0;

    const open   = stmtOpenSignals   ? stmtOpenSignals.all().length                  : 0;
    const recent = stmtRecentSignals ? stmtRecentSignals.all({ n: RECENT_SIGNALS })  : [];
    const agg    = stmtAggSignals    ? stmtAggSignals.get()                          : { greens: 0, reds: 0, open_cnt: open, pnl: 0 };

    const totalDone = (agg.greens || 0) + (agg.reds || 0);
    const acc = totalDone > 0 ? Math.round((agg.greens / totalDone) * 1000) / 10 : 0;

    console.log(`[panel] snapshot: live=${live}, open=${open}, recent=${recent.length}`);
    res.json({
      ok: true,
      live,
      open,
      recent,
      counters: {
        greens: agg.greens || 0,
        reds:   agg.reds   || 0,
        open:   agg.open_cnt || open,
        pnl:    +(agg.pnl || 0).toFixed(2),
        accuracy_pct: acc
      }
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get('/api/open-signals', (req, res) => {
  try {
    if (!stmtOpenSignals) return res.json({ ok: true, rows: [] });
    res.json({ ok: true, rows: stmtOpenSignals.all() });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

app.get('/api/recent-signals', (req, res) => {
  try {
    if (!stmtRecentSignals) return res.json({ ok: true, rows: [] });
    res.json({ ok: true, rows: stmtRecentSignals.all({ n: RECENT_SIGNALS }) });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e) });
  }
});

// front
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.use('/static', express.static(path.join(__dirname, 'static')));

app.listen(PORT, () => {
  console.log('[panel] usando DB:', DB_PATH);
  console.log(`[panel] PRESS_WINDOW_MIN=${PRESS_WINDOW_MIN} min (alinhado ao analisador)`);
  console.log(`[panel] pronto em http://localhost:${PORT}`);
});
