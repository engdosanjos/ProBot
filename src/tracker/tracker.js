const fs = require('fs');
const path = require('path');
const os = require('os');
const EventEmitter = require('events');

const DATA_DIR   = path.resolve(__dirname, '../../data');
const LOGS_DIR   = path.resolve(__dirname, '../../logs');
const STATE_PATH = path.join(DATA_DIR, 'goals_tracker_state.json');
const CSV_PATH   = path.join(LOGS_DIR, 'goals_signals.csv');

const PROFIT_WIN  = 1.1;
const PROFIT_LOSS = -1.0;
const AUTOSAVE_MS = 30_000;

function ensureDir(p) { if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true }); }
function nowISO() { return new Date().toISOString(); }

function appendCSVLine(fields) {
  ensureDir(LOGS_DIR);
  const line = fields.map(v => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    if (/[\",\n]/.test(s)) return `\"${s.replace(/\"/g, '\"\"')}\"`;
    return s;
  }).join(',') + os.EOL;

  const needHeader = !fs.existsSync(CSV_PATH);
  if (needHeader) {
    const header = [
      'timestamp','event','signal_id','match_id','market','odds_over','odds_under',
      'minute_at_signal','horizon','league','home','away',
      'p_model','p_fair','edge','outcome','profit',
      'greens','reds','accuracy','total_profit'
    ].join(',') + os.EOL;
    fs.appendFileSync(CSV_PATH, header, 'utf8');
  }
  fs.appendFileSync(CSV_PATH, line, 'utf8');
}

function defaultState() {
  return {
    created_at: nowISO(),
    updated_at: nowISO(),
    greens: 0,
    reds: 0,
    total_profit: 0.0,
    open_signals: {},
  };
}

class Tracker extends EventEmitter {
  constructor() {
    super();
    ensureDir(DATA_DIR);
    this.state = defaultState();
    this._load();
    this._autosave = setInterval(() => this._save(), AUTOSAVE_MS).unref?.();
  }

  _load() {
    try {
      if (fs.existsSync(STATE_PATH)) {
        const raw = fs.readFileSync(STATE_PATH, 'utf8');
        this.state = JSON.parse(raw);
      }
    } catch (e) {
      console.error('[tracker] erro ao carregar estado, iniciando novo:', e.message);
      this.state = defaultState();
    }
  }

  _save() {
    try {
      this.state.updated_at = nowISO();
      fs.writeFileSync(STATE_PATH, JSON.stringify(this.state, null, 2), 'utf8');
    } catch (e) {
      console.error('[tracker] erro ao salvar estado:', e.message);
    }
  }

  summary(prefix='[tracker]') {
    const total = this.state.greens + this.state.reds;
    const acc = total>0 ? this.state.greens/total : 0;
    console.log(`${prefix} Greens=${this.state.greens} Reds=${this.state.reds} Acc=${(acc*100).toFixed(2)}% P&L=${this.state.total_profit.toFixed(2)}u total=${total}`);
  }

  _uid() {
    return 'S' + Date.now().toString(36) + Math.random().toString(36).slice(2,8);
  }

  openSignal({ match, market, horizon, minuteAt, oddsOver, oddsUnder, p_model, p_fair, edge }) {
    const id = this._uid();
    const baselineGoals = (match.score_home ?? 0) + (match.score_away ?? 0);
    const expiresAtMinute = minuteAt + horizon;
    const signal = {
      id,
      match_id: match.id,
      market,
      horizon,
      minute_at: minuteAt,
      expires_minute: expiresAtMinute,
      odds_over: oddsOver ?? null,
      odds_under: oddsUnder ?? null,
      league: match.league ?? '',
      home: match.home ?? '',
      away: match.away ?? '',
      baseline_goals: baselineGoals,
      p_model, p_fair, edge,
      status: 'OPEN'
    };
    this.state.open_signals[id] = signal;

    appendCSVLine([
      nowISO(),'signal',id,match.id,market,oddsOver,oddsUnder,minuteAt,horizon,
      signal.league,signal.home,signal.away,
      p_model?.toFixed?.(4), p_fair?.toFixed?.(4), edge?.toFixed?.(4),
      '', '', this.state.greens, this.state.reds,
      (this.state.greens+this.state.reds)>0 ? (this.state.greens/(this.state.greens+this.state.reds)).toFixed(4):'0',
      this.state.total_profit.toFixed(2)
    ]);

    this.summary('[tracker:open]');
    this.emit('bet_signal', signal);
    return signal;
  }

  resolveBySnapshot(match) {
    const goalsNow = (match.score_home ?? 0) + (match.score_away ?? 0);
    const minute = match.minute ?? 0;

    for (const id of Object.keys(this.state.open_signals)) {
      const sig = this.state.open_signals[id];
      if (sig.match_id !== match.id || sig.status !== 'OPEN') continue;

      if (goalsNow > sig.baseline_goals && minute <= sig.expires_minute) {
        this._closeSignal(id, 'GREEN', 1.1);
      } else if (minute >= sig.expires_minute) {
        this._closeSignal(id, 'RED', -1.0);
      }
    }
  }

  _closeSignal(id, outcome, profit) {
    const sig = this.state.open_signals[id];
    if (!sig) return;
    sig.status = 'CLOSED';
    sig.outcome = outcome;
    sig.closed_at = nowISO();
    sig.profit = profit;

    if (outcome === 'GREEN') this.state.greens += 1;
    else this.state.reds += 1;
    this.state.total_profit += profit;

    appendCSVLine([
      nowISO(),'result',sig.id,sig.match_id,sig.market,sig.odds_over,sig.odds_under,
      sig.minute_at,sig.horizon,sig.league,sig.home,sig.away,
      sig.p_model?.toFixed?.(4),sig.p_fair?.toFixed?.(4),sig.edge?.toFixed?.(4),
      outcome, profit.toFixed(2),
      this.state.greens, this.state.reds,
      (this.state.greens+this.state.reds)>0 ? (this.state.greens/(this.state.greens+this.state.reds)).toFixed(4):'0',
      this.state.total_profit.toFixed(2)
    ]);

    this.summary('[tracker:close]');
    this.emit('bet_result', { id: sig.id, won: outcome === 'GREEN', market: sig.market, odds: sig.odds_over, minute: sig.minute_at, league: sig.league, home: sig.home, away: sig.away });
    delete this.state.open_signals[id];
    this._save();
  }
}

if (require.main === module) {
  const t = new Tracker();
  t.summary('[tracker:standalone]');
  process.on('SIGINT', () => { console.log('\n[tracker] Ctrl+C -> salvando...'); t._save(); process.exit(0); });
}

module.exports = { Tracker };
