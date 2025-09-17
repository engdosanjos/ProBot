// src/main.js
require('dotenv').config({ override: true });

const path = require('path');
const db = require('./db');
const { startScraper } = require('./scraper/livescore_scraper');

// ---- CONFIG ----
const BASE_URL = process.env.BASE_URL || 'https://www.livescore.in/';
const LIST_URL = process.env.LIST_URL || `${BASE_URL}br/futebol/`;
const HEADLESS = String(process.env.HEADLESS ?? '1') !== '0'; // "1" liga headless; use HEADLESS=0 p/ ver o navegador
const REDISCOVER_MS = Number(process.env.REDISCOVER_EVERY_MS || 60_000);

// handlers úteis p/ diagnosticar travas silenciosas
process.on('unhandledRejection', (err) => {
  console.error('[unhandledRejection]', err);
});
process.on('uncaughtException', (err) => {
  console.error('[uncaughtException]', err);
});

// 1) Inicializa DB (schema + índices + prepared)
if (db && typeof db.init === 'function') {
  db.init();
} else {
  console.warn('[main] db.init() não encontrado — seguindo sem inicialização explícita.');
}
console.log('[main] DB:', db.path);
console.log('[main] boot', { baseUrl: BASE_URL, listUrl: LIST_URL, headless: HEADLESS });

// 2) Sobe o painel depois do DB estar pronto
//    (o server já faz app.listen ao ser requerido)
try {
  require('./panel/server');
  console.log('[main] painel iniciado.');
} catch (e) {
  console.error('[main] falha ao iniciar painel:', e);
}

// 3) Inicia o scraper (ele mesmo grava ticks e chama o analyzer)
startScraper({
  listUrl: LIST_URL,
  headless: HEADLESS,
  rediscoverEveryMs: REDISCOVER_MS
  // Obs.: se a implementação atual do scraper ignorar rediscoverEveryMs/engine,
  // esse parâmetro só ficará aqui como no-op.
}).catch((e) => {
  console.error('[main] fatal:', e);
  process.exit(1);
});
