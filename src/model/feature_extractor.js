class FeatureStore {
  constructor(){ this.matches = new Map(); }

  updateSnapshot(snapshot){
    const s = this.matches.get(snapshot.id) || {
      id: snapshot.id, league: snapshot.league, home: snapshot.home, away: snapshot.away,
      lastMinute: null, lastScore: {h:0,a:0}, goalEvents: [], history: []
    };
    const prevGoals = s.lastScore.h + s.lastScore.a;
    const nowGoals  = (snapshot.score_home ?? 0) + (snapshot.score_away ?? 0);
    const minute    = snapshot.minute ?? 0;
    if (nowGoals > prevGoals && minute!=null){ s.goalEvents.push({ minute, half: snapshot.half, totalGoals: nowGoals }); }
    s.lastMinute = minute;
    s.lastScore = { h: snapshot.score_home ?? 0, a: snapshot.score_away ?? 0 };

    const histPoint = {
      minute, half: snapshot.half,
      sot_h: snapshot.stats?.sot_h ?? null, sot_a: snapshot.stats?.sot_a ?? null,
      cor_h: snapshot.stats?.cor_h ?? null, cor_a: snapshot.stats?.cor_a ?? null,
      da_h:  snapshot.stats?.da_h  ?? null, da_a:  snapshot.stats?.da_a  ?? null,
      score_h: snapshot.score_home ?? 0, score_a: snapshot.score_away ?? 0,
    };
    s.history.push(histPoint); if (s.history.length>300) s.history.shift();
    this.matches.set(snapshot.id, s);
    return { features: this._makeFeatures(snapshot, s), goalsNow: nowGoals };
  }

  _lastNDelta(arr,n,key){
    if (!arr.length) return null; const cur = arr[arr.length-1]; const target = Math.max(0,(cur.minute??0)-n);
    let past = null; for (let i=arr.length-1;i>=0;i--){ if ((arr[i].minute??0)<=target){ past=arr[i]; break; } }
    if (!past) past = arr[0];
    const a = cur[key]; const b = past[key]; if (a==null||b==null) return null;
    return Math.max(0, a-b);
  }
  _recentGoals(events,n){
    if(!events.length) return 0; const curMin = events[events.length-1].minute??0; const start = Math.max(0,curMin-n);
    let c=0; for (let i=events.length-1;i>=0;i--){ const e=events[i]; if ((e.minute??0)>=start) c++; else break; } return c;
  }

  _makeFeatures(snapshot,state){
    const minute = snapshot.minute ?? 0;
    const totalGoals = (snapshot.score_home ?? 0)+(snapshot.score_away ?? 0);
    const diff = (snapshot.score_home ?? 0)-(snapshot.score_away ?? 0);

    const sot10_h = this._lastNDelta(state.history,10,'sot_h');
    const sot10_a = this._lastNDelta(state.history,10,'sot_a');
    const cor10_h = this._lastNDelta(state.history,10,'cor_h');
    const cor10_a = this._lastNDelta(state.history,10,'cor_a');
    const da10_h  = this._lastNDelta(state.history,10,'da_h');
    const da10_a  = this._lastNDelta(state.history,10,'da_a');
    const goals10 = this._recentGoals(state.goalEvents,10);

    return {
      minute_norm: Math.max(0, Math.min(1, minute/45)),
      total_goals: totalGoals,
      score_diff: diff,
      goals_last10: goals10,
      sot10_total: (sot10_h??0)+(sot10_a??0),
      cor10_total: (cor10_h??0)+(cor10_a??0),
      da10_total: (da10_h??0)+(da10_a??0),
      league: snapshot.league || 'unknown',
      home: snapshot.home || 'H?',
      away: snapshot.away || 'A?',
      half: snapshot.half || 1,
    };
  }
}
module.exports = { FeatureStore };
