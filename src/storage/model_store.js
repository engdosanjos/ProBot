// src/storage/model_store.js
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');

const DATA_DIR = path.join(process.cwd(), 'data');
const MODELS_DIR = path.join(DATA_DIR, 'models');
const SNAP_DIR = path.join(DATA_DIR, 'snapshots');
const COUNTERS_FILE = path.join(DATA_DIR, 'counters.json');

function ensureDirSync(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

ensureDirSync(DATA_DIR);
ensureDirSync(MODELS_DIR);
ensureDirSync(SNAP_DIR);

async function readJson(file, fallback = null) {
  try {
    const buf = await fsp.readFile(file, 'utf8');
    return JSON.parse(buf);
  } catch {
    return fallback;
  }
}
async function writeJson(file, obj) {
  await fsp.mkdir(path.dirname(file), { recursive: true }).catch(() => {});
  await fsp.writeFile(file, JSON.stringify(obj, null, 2), 'utf8');
}

async function loadModel(name) {
  const file = path.join(MODELS_DIR, `${name}.json`);
  return await readJson(file, null);
}
async function saveModel(name, payload) {
  const file = path.join(MODELS_DIR, `${name}.json`);
  await writeJson(file, payload);
}

async function loadCounters() {
  const obj = await readJson(COUNTERS_FILE, null);
  if (obj) return obj;
  const init = { green: 0, red: 0, by_market: {}, last_reset_at: Date.now() };
  await writeJson(COUNTERS_FILE, init);
  return init;
}
async function saveCounters(obj) {
  await writeJson(COUNTERS_FILE, obj);
}

module.exports = {
  DATA_DIR, MODELS_DIR, SNAP_DIR,
  loadModel, saveModel,
  loadCounters, saveCounters,
  ensureDirSync
};
