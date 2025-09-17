// src/learn/features.js
function pressureFromDeltas(dh, da) {
  const ph =
    (dh.sot_home || 0) * 3 +
    (dh.soff_home || 0) * 1.5 +
    (dh.da_home || 0) * 0.5 +
    (dh.corners_home || 0) * 0.5;

  const pa =
    (da.sot_away || 0) * 3 +
    (da.soff_away || 0) * 1.5 +
    (da.da_away || 0) * 0.5 +
    (da.corners_away || 0) * 0.5;

  return { ph: +ph.toFixed(2), pa: +pa.toFixed(2) };
}

function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
function bucketMinute(m) { return Math.max(0, Math.floor((m || 0) / 5)); }
function binPdom(v) {
  const x = Math.floor(v);
  if (x <= -3) return -3;
  if (x <= -1) return -1;
  if (x === 0) return 0;
  if (x <= 2) return 2;
  return 3; // >=3
}
function binPsum(v) {
  if (v <= 1) return 1;
  if (v <= 3) return 3;
  if (v <= 6) return 6;
  if (v <= 9) return 9;
  return 12;
}
function binGd(gd) { return clamp(gd, -2, 2); }

module.exports = { pressureFromDeltas, bucketMinute, binPdom, binPsum, binGd };
