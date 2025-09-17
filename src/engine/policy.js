// src/engine/policy.js
const BLOCK_2H_START = 46;
const BLOCK_2H_END   = 50;

const BASE = {
  min_minute: 8,
  max_minute: 82
};

// Thresholds por janela (quanto maior a janela, menor a exigência).
const THRESHOLDS = {
  10: { min_prob: 0.58, min_pressure_00: 0.60, min_pressure_any: 0.80 },
  15: { min_prob: 0.56, min_pressure_00: 0.58, min_pressure_any: 0.75 },
  20: { min_prob: 0.55, min_pressure_00: 0.56, min_pressure_any: 0.72 },
  25: { min_prob: 0.54, min_pressure_00: 0.55, min_pressure_any: 0.70 }
};

function classifyWindow(minute, win) {
  return (minute + win) < 40 ? 'HT' : 'FT';
}

function allowByMinute(minute, half) {
  if (minute < BASE.min_minute || minute > BASE.max_minute) return false;
  if (half === 2 && minute >= BLOCK_2H_START && minute <= BLOCK_2H_END) return false;
  return true;
}

function shouldSignalWindow({ minute, half, totalGoals, pressureTot, prob, win }) {
  if (!allowByMinute(minute, half)) return false;
  const th = THRESHOLDS[win];
  if (!th) return false;
  const needPressure = totalGoals === 0 ? th.min_pressure_00 : th.min_pressure_any;
  if (pressureTot < needPressure) return false;
  return prob >= th.min_prob;
}

function windowCloseMinute(minute, half, win) {
  const label = classifyWindow(minute, win);
  if (label === 'HT') return Math.min(45, minute + win);
  return Math.min(90, minute + win);
}

module.exports = {
  BLOCK_2H_START,
  BLOCK_2H_END,
  classifyWindow,
  shouldSignalWindow,
  windowCloseMinute,
  THRESHOLDS
};
