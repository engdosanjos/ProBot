// src/utils/logger.js
const LOG_PREFIX = 'Log:';

function matchIntro({ home, away, score_home, score_away, league }) {
  const liga = league ? ` — ${league}` : '';
  console.log(`${LOG_PREFIX} ${home} (${score_home}) x (${score_away}) ${away}${liga}`);
}

function entry({ home, away, score_home, score_away, code, windowMin, market, p, minute }) {
  // Ex.: [entry] WIN_15 FT w=15m p=0.61 t=72' — TimeA (1) x (0) TimeB
  const mk = market || '';
  const prob = (p != null) ? p.toFixed(2) : '';
  console.log(`[entry] ${code} ${mk} w=${windowMin}m p=${prob} t=${minute}' — ${home} (${score_home}) x (${score_away}) ${away}`);
}

function result({ home, away, final_home, final_away, betCode, outcome }) {
  // Ex.: [result] GREEN WIN_15 — TimeA (2) x (0) TimeB
  console.log(`[result] ${outcome} ${betCode} — ${home} (${final_home}) x (${final_away}) ${away}`);
}

function matchEnd({ home, away, final_home, final_away }) {
  // Ex.: [final] TimeA (2) x (1) TimeB
  console.log(`[final] ${home} (${final_home}) x (${final_away}) ${away}`);
}

function info(msg) { console.log(msg); }
function warn(msg) { console.warn(msg); }
function error(msg) { console.error(msg); }

module.exports = { matchIntro, entry, result, matchEnd, info, warn, error };
