const cp = require('child_process');

function beep() {
  try { process.stdout.write('\x07'); } catch {}
  if (process.platform === 'win32') {
    try { cp.execSync('powershell -c \"[console]::beep(1000,200)\"'); } catch {}
  }
}

function fairFromOverUnder(oOver, oUnder) {
  if (!oOver || !oUnder) return null;
  const pOver = 1 / oOver;
  const pUnder = 1 / oUnder;
  const margin = pOver + pUnder - 1;
  if (margin <= 0) return pOver;
  return pOver / (1 + margin);
}

const DEFAULTS = {
  minOdds: 1.6,
  edgeHT: 0.05,
  edgeFT: 0.06,
  // quando não há odds: thresholds puros de probabilidade do modelo
  pHT_noOdds: 0.65,
  pFT_noOdds: 0.62,
  ALLOW_SIGNALS_WITHOUT_ODDS: true,
  REQUIRE_ODDS: false,
};

function decide({ minute, probs, odds={}, config = DEFAULTS }) {
  const m = minute ?? null;
  if (m == null) return null;

  const requireOdds = (config.REQUIRE_ODDS ?? DEFAULTS.REQUIRE_ODDS);
  const allowNoOdds = (config.ALLOW_SIGNALS_WITHOUT_ODDS ?? DEFAULTS.ALLOW_SIGNALS_WITHOUT_ODDS);

  // Janela HT: 15-25min com p(10')
  if (m >= 15 && m <= 25 && probs.p10 != null) {
    const hasOdds = odds.over05_ht && odds.under05_ht;
    if (hasOdds) {
      const p_fair = fairFromOverUnder(odds.over05_ht, odds.under05_ht);
      if (p_fair != null) {
        const edge = probs.p10 - p_fair;
        if (edge >= (config.edgeHT ?? DEFAULTS.edgeHT) && odds.over05_ht >= (config.minOdds ?? DEFAULTS.minOdds)) {
          return { market: 'HT', horizon: 10, p_model: probs.p10, p_fair, edge, oddsOver: odds.over05_ht, oddsUnder: odds.under05_ht };
        }
      }
    } else if (!requireOdds && allowNoOdds && probs.p10 >= (config.pHT_noOdds ?? DEFAULTS.pHT_noOdds)) {
      return { market: 'HT', horizon: 10, p_model: probs.p10, p_fair: null, edge: null, oddsOver: null, oddsUnder: null };
    }
  }

  // Janela FT: 25-38min com p(20')
  if (m >= 25 && m <= 38 && probs.p20 != null) {
    const hasOdds = odds.over05_ft && odds.under05_ft;
    if (hasOdds) {
      const p_fair = fairFromOverUnder(odds.over05_ft, odds.under05_ft);
      if (p_fair != null) {
        const edge = probs.p20 - p_fair;
        if (edge >= (config.edgeFT ?? DEFAULTS.edgeFT) && odds.over05_ft >= (config.minOdds ?? DEFAULTS.minOdds)) {
          return { market: 'FT', horizon: 20, p_model: probs.p20, p_fair, edge, oddsOver: odds.over05_ft, oddsUnder: odds.under05_ft };
        }
      }
    } else if (!requireOdds && allowNoOdds && probs.p20 >= (config.pFT_noOdds ?? DEFAULTS.pFT_noOdds)) {
      return { market: 'FT', horizon: 20, p_model: probs.p20, p_fair: null, edge: null, oddsOver: null, oddsUnder: null };
    }
  }

  return null;
}

module.exports = { decide, beep, fairFromOverUnder, DEFAULTS };
