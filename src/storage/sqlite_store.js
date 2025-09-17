// src/storage/sqlite_store.js
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

class SqliteStore {
  constructor(dbPath) {
    const dir = path.dirname(dbPath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.db.pragma('temp_store = MEMORY');

    this._initSchema();
    this._prep();
  }

  _initSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS leagues (
        id    INTEGER PRIMARY KEY AUTOINCREMENT,
        name  TEXT UNIQUE NOT NULL
      );

      CREATE TABLE IF NOT EXISTS teams (
        id    INTEGER PRIMARY KEY AUTOINCREMENT,
        name  TEXT UNIQUE NOT NULL
      );

      CREATE TABLE IF NOT EXISTS matches (
        event_id      TEXT PRIMARY KEY,
        url           TEXT,
        league_id     INTEGER,
        home_team_id  INTEGER,
        away_team_id  INTEGER,
        started_at    INTEGER,
        last_seen_at  INTEGER,
        status        TEXT,
        goals_home    INTEGER,
        goals_away    INTEGER,
        FOREIGN KEY (league_id)    REFERENCES leagues(id),
        FOREIGN KEY (home_team_id) REFERENCES teams(id),
        FOREIGN KEY (away_team_id) REFERENCES teams(id)
      );

      CREATE TABLE IF NOT EXISTS snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT NOT NULL,
        ts       INTEGER NOT NULL,   -- Date.now()
        minute   INTEGER,
        status   TEXT,
        goals_home INTEGER,
        goals_away INTEGER,

        st_home    INTEGER, st_away    INTEGER,
        sot_home   INTEGER, sot_away   INTEGER,
        soff_home  INTEGER, soff_away  INTEGER,
        da_home    INTEGER, da_away    INTEGER,
        corners_home INTEGER, corners_away INTEGER,

        d_st_home    INTEGER, d_st_away    INTEGER,
        d_sot_home   INTEGER, d_sot_away   INTEGER,
        d_soff_home  INTEGER, d_soff_away  INTEGER,
        d_da_home    INTEGER, d_da_away    INTEGER,
        d_corners_home INTEGER, d_corners_away INTEGER,

        press_home REAL, 
        press_away REAL,

        FOREIGN KEY (event_id) REFERENCES matches(event_id)
      );

      CREATE INDEX IF NOT EXISTS idx_snap_event_ts   ON snapshots(event_id, ts);
      CREATE INDEX IF NOT EXISTS idx_snap_event_min  ON snapshots(event_id, minute);
      CREATE INDEX IF NOT EXISTS idx_match_league    ON matches(league_id);
      CREATE INDEX IF NOT EXISTS idx_match_home_away ON matches(home_team_id, away_team_id);
    `);
  }

  _prep() {
    this.insLeague   = this.db.prepare(`INSERT OR IGNORE INTO leagues(name) VALUES(?)`);
    this.selLeagueId = this.db.prepare(`SELECT id FROM leagues WHERE name = ?`);

    this.insTeam     = this.db.prepare(`INSERT OR IGNORE INTO teams(name) VALUES(?)`);
    this.selTeamId   = this.db.prepare(`SELECT id FROM teams WHERE name = ?`);

    this.insMatch    = this.db.prepare(`
      INSERT OR IGNORE INTO matches(event_id, url, league_id, home_team_id, away_team_id, started_at, status, last_seen_at, goals_home, goals_away)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    this.updMatch    = this.db.prepare(`
      UPDATE matches
         SET url = COALESCE(?, url),
             league_id = COALESCE(?, league_id),
             home_team_id = COALESCE(?, home_team_id),
             away_team_id = COALESCE(?, away_team_id),
             status = COALESCE(?, status),
             last_seen_at = COALESCE(?, last_seen_at),
             goals_home = COALESCE(?, goals_home),
             goals_away = COALESCE(?, goals_away)
       WHERE event_id = ?
    `);

    this.insSnap     = this.db.prepare(`
      INSERT INTO snapshots(
        event_id, ts, minute, status, goals_home, goals_away,
        st_home, st_away, sot_home, sot_away, soff_home, soff_away, da_home, da_away, corners_home, corners_away,
        d_st_home, d_st_away, d_sot_home, d_sot_away, d_soff_home, d_soff_away, d_da_home, d_da_away, d_corners_home, d_corners_away,
        press_home, press_away
      )
      VALUES (?, ?, ?, ?, ?, ?,
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
              ?, ?)
    `);

    this.txUpsertMatch = this.db.transaction((payload) => {
      const leagueId = this._upsertLeague(payload.league);
      const homeId   = this._upsertTeam(payload.home);
      const awayId   = this._upsertTeam(payload.away);

      this.insMatch.run(
        payload.event_id, payload.url || null, leagueId, homeId, awayId,
        payload.started_at || null, payload.status || null, Date.now(),
        Number.isFinite(payload.goals_home) ? payload.goals_home : null,
        Number.isFinite(payload.goals_away) ? payload.goals_away : null
      );

      this.updMatch.run(
        payload.url || null, leagueId, homeId, awayId, payload.status || null,
        Date.now(),
        Number.isFinite(payload.goals_home) ? payload.goals_home : null,
        Number.isFinite(payload.goals_away) ? payload.goals_away : null,
        payload.event_id
      );
    });

    this.txInsertSnapshot = this.db.transaction((evId, snap) => {
      this.insSnap.run(
        evId, snap.ts, snap.minute, snap.status, snap.goals_home, snap.goals_away,
        snap.st_home,  snap.st_away,  snap.sot_home,  snap.sot_away,
        snap.soff_home, snap.soff_away, snap.da_home,   snap.da_away,
        snap.corners_home, snap.corners_away,
        snap.d_st_home,  snap.d_st_away,  snap.d_sot_home,  snap.d_sot_away,
        snap.d_soff_home, snap.d_soff_away, snap.d_da_home,   snap.d_da_away,
        snap.d_corners_home, snap.d_corners_away,
        snap.press_home, snap.press_away
      );
      // mantém match “vivo”
      this.updMatch.run(null, null, null, null, snap.status || null, Date.now(),
        Number.isFinite(snap.goals_home) ? snap.goals_home : null,
        Number.isFinite(snap.goals_away) ? snap.goals_away : null,
        evId
      );
    });
  }

  _upsertLeague(name) {
    if (!name) return null;
    this.insLeague.run(name);
    const row = this.selLeagueId.get(name);
    return row?.id ?? null;
  }
  _upsertTeam(name) {
    if (!name) return null;
    this.insTeam.run(name);
    const row = this.selTeamId.get(name);
    return row?.id ?? null;
  }

  upsertMatch(meta) {
    // meta: {event_id, url, league, home, away, goals_home?, goals_away?, status?, started_at?}
    this.txUpsertMatch(meta);
  }

  insertSnapshot(eventId, snap) {
    // snap: (campos da tabela snapshots)
    this.txInsertSnapshot(eventId, snap);
  }

  close() {
    try { this.db.close(); } catch {}
  }
}

module.exports = { SqliteStore };
