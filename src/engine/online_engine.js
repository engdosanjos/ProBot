// src/engine/online_engine.js
// Recebe os snapshots do scraper e grava no SQLite, evitando inflar o BD.

function pressureScoreFromDeltas(dh, da) {
  const ph =
    (dh.sot_home || 0)  * 3   +
    (dh.soff_home || 0) * 1.5 +
    (dh.da_home || 0)   * 0.5 +
    (dh.corners_home||0)* 0.5;

  const pa =
    (da.sot_away || 0)  * 3   +
    (da.soff_away || 0) * 1.5 +
    (da.da_away || 0)   * 0.5 +
    (da.corners_away||0)* 0.5;

  return { home: +ph.toFixed(2), away: +pa.toFixed(2) };
}

class OnlineEngine {
  /**
   * @param {object} opts
   * @param {SqliteStore} opts.store
   * @param {boolean} [opts.saveEveryTick=false]  // força gravar todos os ticks
   */
  constructor({ store, saveEveryTick = false } = {}) {
    this.store = store;
    this.saveEveryTick = !!saveEveryTick;
    this._lastSeen = new Map(); // event_id -> {minute,lastTotals}
  }

  async registerMatch({ eventId, url, header }) {
    // header: {home, away, league, status, minute, goalsHome, goalsAway}
    const meta = {
      event_id: eventId,
      url,
      league: header.league || null,
      home:   header.home   || null,
      away:   header.away   || null,
      goals_home: Number.isFinite(header.goalsHome) ? header.goalsHome : null,
      goals_away: Number.isFinite(header.goalsAway) ? header.goalsAway : null,
      status: header.status || null,
      started_at: Date.now()
    };
    this.store.upsertMatch(meta);
    this._lastSeen.set(eventId, { minute: header.minute ?? null });
  }

  // chamado pelo scraper em cada tick
  async onSnapshot(payload) {
    // payload: { event_id, home, away, league, status, minute, goals_home, goals_away,
    //            st_home, st_away, sot_home, sot_away, soff_home, soff_away,
    //            da_home, da_away, corners_home, corners_away, deltas? }
    const ev  = payload.event_id;
    if (!ev) return;

    // garante match atualizado (liga/time podem variar por tradução/slug)
    this.store.upsertMatch({
      event_id: ev,
      url: payload.url || null,
      league: payload.league || null,
      home: payload.home || null,
      away: payload.away || null,
      goals_home: Number.isFinite(payload.goals_home) ? payload.goals_home : null,
      goals_away: Number.isFinite(payload.goals_away) ? payload.goals_away : null,
      status: payload.status || null,
    });

    const last = this._lastSeen.get(ev) || { minute: null };
    const lastMinute = last.minute;

    // deltas vieram do scraper; se não, calcula “diferença bruta 0”
    const d = payload.deltas || null;

    const dh = {
      st_home: d?.st_home || 0,
      sot_home: d?.sot_home || 0,
      soff_home: d?.soff_home || 0,
      da_home: d?.da_home || 0,
      corners_home: d?.corners_home || 0,
    };
    const da = {
      st_away: d?.st_away || 0,
      sot_away: d?.sot_away || 0,
      soff_away: d?.soff_away || 0,
      da_away: d?.da_away || 0,
      corners_away: d?.corners_away || 0,
    };

    const press = pressureScoreFromDeltas(dh, da);
    const minuteChanged = (payload.minute ?? null) !== lastMinute;
    const hasDelta = !!(d && Object.values(d).some(v => (v || 0) !== 0));

    if (this.saveEveryTick || hasDelta || minuteChanged) {
      this.store.insertSnapshot(ev, {
        ts: Date.now(),
        minute: payload.minute ?? null,
        status: payload.status || null,
        goals_home: Number.isFinite(payload.goals_home) ? payload.goals_home : null,
        goals_away: Number.isFinite(payload.goals_away) ? payload.goals_away : null,

        st_home: payload.st_home || 0,   st_away: payload.st_away || 0,
        sot_home: payload.sot_home || 0, sot_away: payload.sot_away || 0,
        soff_home: payload.soff_home || 0, soff_away: payload.soff_away || 0,
        da_home: payload.da_home || 0,   da_away: payload.da_away || 0,
        corners_home: payload.corners_home || 0, corners_away: payload.corners_away || 0,

        d_st_home: dh.st_home, d_st_away: da.st_away,
        d_sot_home: dh.sot_home, d_sot_away: da.sot_away,
        d_soff_home: dh.soff_home, d_soff_away: da.soff_away,
        d_da_home: dh.da_home, d_da_away: da.da_away,
        d_corners_home: dh.corners_home, d_corners_away: da.corners_away,

        press_home: press.home,
        press_away: press.away,
      });
      this._lastSeen.set(ev, { minute: payload.minute ?? lastMinute });
    }
  }

  async onMatchEnd(payload) {
    // payload: {event_id, status, minute, goals_home, goals_away, ...}
    if (!payload?.event_id) return;
    this.store.upsertMatch({
      event_id: payload.event_id,
      status: payload.status || 'Encerrado',
      goals_home: Number.isFinite(payload.goals_home) ? payload.goals_home : null,
      goals_away: Number.isFinite(payload.goals_away) ? payload.goals_away : null,
    });
    this._lastSeen.delete(payload.event_id);
  }

  // opcional — só para telemetry
  async onDiscoverySnapshot(_) { /* noop */ }
}

module.exports = { OnlineEngine };
