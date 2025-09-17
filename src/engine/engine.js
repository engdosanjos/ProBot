// src/engine/engine.js
const logger = require('../utils/logger');

const POLICY = {
  pre_window_min: 6,
  min_prob_to_bet: 0.60,
  avoid_second_half_block: [46, 50],
};

function pressureFrom(statsDeltaPerMin) {
  const { dSOT=0, dSOFF=0, dDA=0, dCOR=0 } = statsDeltaPerMin || {};
  const W_SOT = 1.0, W_SOFF = 0.5, W_DA = 0.05, W_COR = 0.6;
  return W_SOT*dSOT + W_SOFF*dSOFF + W_DA*dDA + W_COR*dCOR;
}

function probFromFeatures({ pressure, minute, is00 }) {
  const base = Math.max(0, Math.min(1, 0.1 + 0.02 * pressure));
  const midBoost = (minute >= 20 && minute <= 75) ? 0.05 : 0;
  const zeroZeroPenalty = is00 ? -0.02 : 0;
  return Math.max(0, Math.min(1, base + midBoost + zeroZeroPenalty));
}

class OnlineModel {
  constructor(store) { this.store = store; }
  update({ contextKey, green }) {
    const st = this.store[contextKey] || { value: 0, n: 0 };
    st.value += green ? 1.1 : -1.0;
    st.n += 1;
    this.store[contextKey] = st;
  }
  adjustThreshold(contextKey, base) {
    const st = this.store[contextKey];
    if (!st || st.n < 10) return base;
    const adj = Math.max(-0.05, Math.min(0.05, st.value / (50 * st.n)));
    return Math.max(0.50, Math.min(0.75, base - adj));
  }
}

class Engine {
  constructor({ modelStore }) {
    this.model = new OnlineModel(modelStore || {});
    this.matches = new Map();
  }

  onSnapshot(snap) {
    let st = this.matches.get(snap.id);
    if (!st) {
      st = {
        hist: [],
        openSignals: new Map(),
        context: { league: snap.league, home: snap.home, away: snap.away }
      };
      this.matches.set(snap.id, st);
    }

    st.hist.push({ minute: snap.minute, ...snap.stats });
    if (st.hist.length > 60) st.hist.shift();

    const isInterval = /Intervalo/i.test(snap.status);
    const is2HBlock = (snap.minute >= POLICY.avoid_second_half_block[0] && snap.minute <= POLICY.avoid_second_half_block[1]);
    if (isInterval || is2HBlock) return;

    this.maybeSignalWindows(snap, st);
    this.maybeCloseSignals(snap, st);

    if (/Encerrado/i.test(snap.status)) {
      logger.matchEnd({
        home: snap.home, away: snap.away,
        final_home: snap.score_home, final_away: snap.score_away
      });
      this.matches.delete(snap.id);
    }
  }

  _deltaWindow(st, nowMinute) {
    const win = POLICY.pre_window_min;
    const baseline = Math.max(0, nowMinute - win);
    const baseRec = [...st.hist].reverse().find(r => r.minute <= baseline) || st.hist[0];
    const curRec = st.hist[st.hist.length - 1] || baseRec;

    const d = (a,b)=> Math.max(0, (a||0) - (b||0));
    const minutes = Math.max(1, curRec.minute - (baseRec?.minute || curRec.minute));
    const dSOT_h = d(curRec.sot_h, baseRec?.sot_h), dSOT_a = d(curRec.sot_a, baseRec?.sot_a);
    const dSOF_h = d(curRec.soff_h, baseRec?.soff_h), dSOF_a = d(curRec.soff_a, baseRec?.soff_a);
    const dDA_h  = d(curRec.da_h,  baseRec?.da_h),  dDA_a  = d(curRec.da_a,  baseRec?.da_a);
    const dCOR_h = d(curRec.cor_h, baseRec?.cor_h), dCOR_a = d(curRec.cor_a, baseRec?.cor_a);

    const dSOT = (dSOT_h + dSOT_a) / minutes;
    const dSOFF= (dSOF_h + dSOF_a) / minutes;
    const dDA  = (dDA_h  + dDA_a ) / minutes;
    const dCOR = (dCOR_h + dCOR_a) / minutes;

    return { dSOT, dSOFF, dDA, dCOR, minutes };
  }

  _marketFor(minute, windowMin){
    return (minute + windowMin) < 40 ? 'HT' : 'FT';
  }

  maybeSignalWindows(snap, st) {
    const { minute } = snap;
    if (minute <= 5 || minute >= 88) return;

    const d = this._deltaWindow(st, minute);
    const pressure = pressureFrom(d);
    const is00 = (snap.score_home + snap.score_away) === 0;

    const p10 = probFromFeatures({ pressure, minute, is00 });
    const p15 = probFromFeatures({ pressure: pressure*1.05, minute, is00 });
    const p20 = probFromFeatures({ pressure: pressure*1.1,  minute, is00 });
    const p25 = probFromFeatures({ pressure: pressure*1.15, minute, is00 });

    const contextKey = `${snap.league}|${is00?'00':'NG'}`;
    const cut = this.model.adjustThreshold(contextKey, POLICY.min_prob_to_bet);

    const tryOpen = (code, p, windowMin) => {
      if (p >= cut && !st.openSignals.has(code)) {
        const market = this._marketFor(minute, windowMin);
        st.openSignals.set(code, { code, windowMin, openedAtMin: minute, market });
        logger.entry({
          home: snap.home, away: snap.away,
          score_home: snap.score_home, score_away: snap.score_away,
          code, windowMin, market, p, minute
        });
        // registra gols no momento da entrada
        st._goalsAtOpen = (snap.score_home + snap.score_away);
      }
    };

    tryOpen('WIN_10', p10, 10);
    tryOpen('WIN_15', p15, 15);
    tryOpen('WIN_20', p20, 20);
    tryOpen('WIN_25', p25, 25);
  }

  maybeCloseSignals(snap, st) {
    const totalGoals = snap.score_home + snap.score_away;
    for (const [key, sig] of [...st.openSignals.entries()]) {
      const expired = (snap.minute - sig.openedAtMin) >= sig.windowMin;
      if (expired) {
        const entryGoals = (st._goalsAtOpen ?? totalGoals);
        const green = (totalGoals > entryGoals);

        logger.result({
          home: snap.home, away: snap.away,
          final_home: snap.score_home, final_away: snap.score_away,
          betCode: sig.code, outcome: green ? 'GREEN' : 'RED'
        });

        const contextKey = `${snap.league}|${(snap.score_home+snap.score_away)===0?'00':'NG'}`;
        this.model.update({ contextKey, green });

        st.openSignals.delete(key);
      }
    }
  }
}

module.exports = { Engine };
