// src/scraper/livescore_scraper.js
// Leitura e monitoramento de jogos ao vivo do LiveScore.in
// - Abre lista "Ao Vivo", coleta links de jogos ao vivo
// - Abre cada jogo em sequência; espera 2s e inicia monitor
// - Fecha monitor se não houver estatísticas
// - Redescobre a lista a cada REDISCOVER_MS
// - Coleta estatísticas a cada POLL_MS e salva cada tick no DB
// - Calcula PRESSÃO como Δ (6min por padrão) usando minutos de jogo (não relógio)

const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

// ========================= CONFIG =========================
const LIST_URL                = process.env.LIST_URL || 'https://www.livescore.in/br/futebol/';
const HEADLESS                = String(process.env.HEADLESS || '1') !== '0';
const REDISCOVER_MS           = Math.max(60_000, Number(process.env.REDISCOVER_MS || 600_000)); // 10min
const POLL_MS                 = Math.max(3_000, Number(process.env.STATS_POLL_MS || 20_000));   // 20s
const NAV_TIMEOUT_MS          = Math.max(15_000, Number(process.env.NAV_TIMEOUT_MS || 30_000));
const FIRST_PAINT_TIMEOUT_MS  = Math.max(10_000, Number(process.env.FIRST_PAINT_TIMEOUT_MS || 40_000));
const FILTER_WAIT_MS          = Math.max(3_000, Number(process.env.FILTER_WAIT_MS || 8_000));   // aguardo para tabs
const DETAIL_URL_PREFIX       = 'https://www.livescore.in';
const DETAIL_STATS_SUFFIX     = '#/resumo-de-jogo/estatisticas-de-jogo/0';
const LOG_BASIC               = String(process.env.LOG_BASIC || '1') === '1';
const DB_PATH                 = process.env.DB_PATH || path.join(__dirname, '..', '..', 'data', 'events.db');
const JSONL_DIR               = path.join(__dirname, '..', '..', 'data', 'ticks');
const DETAIL_UA               = 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1';

// janela para calcular pressão por Δ de estatísticas (em MINUTOS DE JOGO)
const PRESS_WINDOW_MIN        = Math.max(1, Number(process.env.PRESS_WINDOW_MIN || 6));

// ========================= DB LAYER =========================
function ensureDir(p) { try { fs.mkdirSync(p, { recursive: true }); } catch {} }

// migração leve para novas colunas
function ensureTickColsBetter(db) {
  const add = (name, type='INTEGER') => {
    try { db.exec(`ALTER TABLE ticks ADD COLUMN ${name} ${type};`); } catch {}
  };
  add('sblk_home'); add('sblk_away');
  add('bc_home');   add('bc_away');
  add('xg_home','REAL'); add('xg_away','REAL');
}
function ensureTickColsSqlite3(db) {
  const add = (name, type='INTEGER') => {
    db.run(`ALTER TABLE ticks ADD COLUMN ${name} ${type};`, (/*err*/) => {/*ignora*/});
  };
  add('sblk_home'); add('sblk_away');
  add('bc_home');   add('bc_away');
  add('xg_home','REAL'); add('xg_away','REAL');
}

function makeDb(dbPath, jsonlRoot) {
  // tenta better-sqlite3 -> sqlite3 -> fallback JSONL
  try {
    const Better = require('better-sqlite3');
    const db = new Better(dbPath);
    db.pragma('journal_mode = WAL', { simple: true });
    db.exec(`
      CREATE TABLE IF NOT EXISTS matches (
        event_id   TEXT PRIMARY KEY,
        url        TEXT,
        league     TEXT,
        home       TEXT,
        away       TEXT,
        created_at INTEGER
      );
      CREATE TABLE IF NOT EXISTS ticks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT,
        ts INTEGER,
        minute INTEGER,
        status TEXT,
        goals_home INTEGER,
        goals_away INTEGER,
        st_home INTEGER, st_away INTEGER,
        sot_home INTEGER, sot_away INTEGER,
        soff_home INTEGER, soff_away INTEGER,
        da_home INTEGER, da_away INTEGER,
        corners_home INTEGER, corners_away INTEGER
      );
    `);
    // garante colunas novas ANTES dos prepares
    ensureTickColsBetter(db);

    const upsertMatch = db.prepare(`
      INSERT INTO matches(event_id,url,league,home,away,created_at)
      VALUES(@event_id,@url,@league,@home,@away,@created_at)
      ON CONFLICT(event_id) DO UPDATE SET
        url=excluded.url, league=excluded.league, home=excluded.home, away=excluded.away
    `);
    const insertTick = db.prepare(`
      INSERT INTO ticks(event_id,ts,minute,status,goals_home,goals_away,
        st_home,st_away,sot_home,sot_away,soff_home,soff_away,da_home,da_away,corners_home,corners_away,
        sblk_home,sblk_away,bc_home,bc_away,xg_home,xg_away)
      VALUES(@event_id,@ts,@minute,@status,@goals_home,@goals_away,
        @st_home,@st_away,@sot_home,@sot_away,@soff_home,@soff_away,@da_home,@da_away,@corners_home,@corners_away,
        @sblk_home,@sblk_away,@bc_home,@bc_away,@xg_home,@xg_away)
    `);
    if (LOG_BASIC) console.log('[db] usando better-sqlite3:', dbPath);
    return {
      type: 'sqlite',
      upsertMatch: (m) => upsertMatch.run(m),
      insertTick:  (t) => insertTick.run(t),
      close: () => { try { db.close(); } catch {} },
    };
  } catch {}

  try {
    const sqlite3 = require('sqlite3').verbose();
    ensureDir(path.dirname(dbPath));
    const db = new sqlite3.Database(dbPath);
    if (LOG_BASIC) console.log('[db] usando sqlite3:', dbPath);
    db.serialize(() => {
      db.run(`CREATE TABLE IF NOT EXISTS matches (
        event_id TEXT PRIMARY KEY, url TEXT, league TEXT, home TEXT, away TEXT, created_at INTEGER
      );`);
      db.run(`CREATE TABLE IF NOT EXISTS ticks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT, ts INTEGER, minute INTEGER, status TEXT, goals_home INTEGER, goals_away INTEGER,
        st_home INTEGER, st_away INTEGER, sot_home INTEGER, sot_away INTEGER, soff_home INTEGER, soff_away INTEGER,
        da_home INTEGER, da_away INTEGER, corners_home INTEGER, corners_away INTEGER
      );`);
      // garante colunas novas
      ensureTickColsSqlite3(db);
    });
    const upsertMatch = (m) => new Promise((res, rej) => {
      db.run(
        `INSERT INTO matches(event_id,url,league,home,away,created_at)
         VALUES(?,?,?,?,?,?)
         ON CONFLICT(event_id) DO UPDATE SET url=excluded.url, league=excluded.league, home=excluded.home, away=excluded.away`,
        [m.event_id, m.url, m.league, m.home, m.away, m.created_at],
        (err) => err ? rej(err) : res()
      );
    });
    const insertTick = (t) => new Promise((res, rej) => {
      db.run(
        `INSERT INTO ticks(event_id,ts,minute,status,goals_home,goals_away,
          st_home,st_away,sot_home,sot_away,soff_home,soff_away,da_home,da_away,corners_home,corners_away,
          sblk_home,sblk_away,bc_home,bc_away,xg_home,xg_away)
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
        [
          t.event_id, t.ts, t.minute, t.status, t.goals_home, t.goals_away,
          t.st_home, t.st_away, t.sot_home, t.sot_away, t.soff_home, t.soff_away,
          t.da_home, t.da_away, t.corners_home, t.corners_away,
          t.sblk_home, t.sblk_away, t.bc_home, t.bc_away, t.xg_home, t.xg_away
        ],
        (err) => err ? rej(err) : res()
      );
    });
    return {
      type: 'sqlite3',
      upsertMatch,
      insertTick,
      close: () => { try { db.close(); } catch {} },
    };
  } catch {}

  // fallback JSONL
  ensureDir(jsonlRoot);
  let currentFile = '';
  const ensureFile = () => {
    const d = new Date();
    const name = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}.jsonl`;
    currentFile = path.join(jsonlRoot, name);
    return currentFile;
  };
  if (LOG_BASIC) console.log('[db] fallback JSONL em', jsonlRoot);
  return {
    type: 'jsonl',
    upsertMatch: (m) => {
      const metaFile = path.join(jsonlRoot, 'matches-meta.jsonl');
      fs.appendFileSync(metaFile, JSON.stringify({ _type:'match', ...m })+'\n');
    },
    insertTick: (t) => {
      const f = ensureFile();
      fs.appendFileSync(f, JSON.stringify({ _type: 'tick', ...t })+'\n');
    },
    close: () => {},
  };
}

// ========================= HELPERS =========================
function parseEventIdFromHref(href) {
  const m = /\/jogo\/futebol\/([A-Za-z0-9]+)\//.exec(href || '');
  return m ? m[1] : null;
}

function normalizeDetailUrl(u) {
  if (!u) return null;
  const abs = u.startsWith('http') ? u : (DETAIL_URL_PREFIX + u);
  if (abs.includes('#/resumo-de-jogo') && !abs.includes('estatisticas-de-jogo')) {
    return abs.split('#')[0] + DETAIL_STATS_SUFFIX;
  }
  if (!abs.includes('#/')) return abs + DETAIL_STATS_SUFFIX;
  return abs;
}

async function acceptCookiesIfAny(page) {
  const candidates = [
    "//button[span[contains(., 'Aceitar') or contains(., 'Concordo') or contains(., 'OK')]]",
    "button:has-text('Aceitar')",
    "button:has-text('Concordo')",
    "button:has-text('OK')",
    "button:has-text('Aceitar todos')",
  ];
  for (const sel of candidates) {
    try {
      const btn = page.locator(sel);
      if (await btn.count()) {
        if (await btn.first().isVisible()) {
          await btn.first().click({ timeout: 1000 }).catch(()=>{});
          await page.waitForTimeout(200);
          break;
        }
      }
    } catch {}
  }
}

// ——— Ativar aba "Ao Vivo" de forma robusta ———
async function ensureLiveFilter(page) {
  // 0) garante que a SPA teve tempo de hidratar
  await page.waitForLoadState('domcontentloaded').catch(()=>{});
  await page.waitForLoadState('networkidle', { timeout: 8000 }).catch(()=>{});
  await page.waitForTimeout(200);

  // helper: já está selecionado?
  const isAoVivoSelected = async () => {
    try {
      const sel = page.locator('.filters__tab.selected .filters__text', { hasText: /ao\s*vivo/i });
      return (await sel.count()) > 0;
    } catch { return false; }
  };

  // 1) tenta pela UI atual (.filters__group/.filters__tab)
  try {
    await page.waitForSelector('.filters__group .filters__tab', { timeout: FILTER_WAIT_MS });
  } catch {}
  if (await isAoVivoSelected()) {
    if (LOG_BASIC) console.log('[scraper] filtro Ao Vivo já selecionado (.filters__tab.selected).');
    return true;
  }

  const aoVivoTab = page.locator(
    '.filters__group .filters__tab',
    { has: page.locator('.filters__text', { hasText: /ao\s*vivo/i }) }
  ).first();

  if ((await aoVivoTab.count()) > 0) {
    try {
      await aoVivoTab.scrollIntoViewIfNeeded().catch(()=>{});
      const textChild = aoVivoTab.locator('.filters__text', { hasText: /ao\s*vivo/i }).first();

      let clicked = false;
      try {
        await textChild.click({ timeout: 3000 });
        clicked = true;
      } catch {
        try { await aoVivoTab.click({ timeout: 3000 }); clicked = true; } catch {}
      }

      if (clicked) {
        // espera a classe selected mudar
        const ok = await Promise.race([
          page.waitForSelector('.filters__tab.selected .filters__text', { hasText: /ao\s*vivo/i, timeout: 2000 }).then(() => true).catch(() => false),
          page.waitForSelector("div.event__match.event__match--live[data-event-row='true']", { timeout: 2000 }).then(() => true).catch(() => false),
        ]);
        if (ok || await isAoVivoSelected()) {
          if (LOG_BASIC) console.log('[scraper] ativou filtro: Ao Vivo (via .filters__tab/.filters__text).');
          return true;
        }
      }
    } catch (e) {
      if (LOG_BASIC) console.log('[scraper] erro ao ativar "Ao Vivo" (.filters__tab):', e.message || e);
    }

    // fallback direto no DOM (às vezes o handler está no nó original)
    try {
      const clickedDom = await page.evaluate(() => {
        const group = document.querySelector('.filters__group');
        if (!group) return false;
        const tabs = Array.from(group.querySelectorAll('.filters__tab'));
        const tab = tabs.find(t => /ao\s*vivo/i.test((t.innerText || '').trim()));
        if (!tab) return false;
        const target = tab.querySelector('.filters__text') || tab;
        target.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window }));
        return true;
      });
      if (clickedDom) {
        await page.waitForTimeout(500);
        if (await isAoVivoSelected()) {
          if (LOG_BASIC) console.log('[scraper] ativou filtro: Ao Vivo (via evaluate).');
          return true;
        }
      }
    } catch {}
  }

  // 2) Fallback para layout antigo (wcl-tabs)
  const wclBtn = page.locator('[data-testid="wcl-tabs"] [data-testid="wcl-tab"]', { hasText: /ao\s*vivo/i }).first();
  if (await wclBtn.count()) {
    try {
      await wclBtn.scrollIntoViewIfNeeded().catch(()=>{});
      await wclBtn.click({ timeout: 3000 });
      const ok = await Promise.race([
        page.waitForFunction(() => {
          const sel = document.querySelector('[data-testid="wcl-tabs"] [data-testid="wcl-tab"][data-selected="true"]');
          return !!(sel && /ao\s*vivo/i.test(sel.textContent || ''));
        }, { timeout: 2000 }).then(() => true).catch(() => false),
        page.waitForSelector("div.event__match.event__match--live[data-event-row='true']", { timeout: 2000 }).then(() => true).catch(() => false),
      ]);
      if (ok) {
        if (LOG_BASIC) console.log('[scraper] ativou filtro: Ao Vivo (via wcl-tabs).');
        return true;
      }
    } catch {}
  }

  // 3) Último fallback por texto
  try {
    const cand = page.locator('text=/\\bAo\\s*Vivo\\b/i').first();
    if (await cand.count()) {
      await cand.scrollIntoViewIfNeeded().catch(()=>{});
      await cand.click({ timeout: 3000 }).catch(()=>{});
      const ok = await page.waitForSelector("div.event__match.event__match--live[data-event-row='true']", { timeout: 3000 }).then(() => true).catch(() => false);
      if (ok || await isAoVivoSelected()) {
        if (LOG_BASIC) console.log('[scraper] ativou filtro: Ao Vivo (fallback texto).');
        return true;
      }
    }
  } catch {}

  if (LOG_BASIC) console.log('[scraper] aviso: não encontrei/ativei o botão "Ao Vivo" (seguindo mesmo assim).');
  return false;
}

async function collectLiveLinks(page) {
  const hrefs = await page.evaluate(() => {
    const out = new Set();
    const rowsA = Array.from(document.querySelectorAll("div.event__match.event__match--live[data-event-row='true'] a.eventRowLink"));
    rowsA.forEach(a => { const h = a.getAttribute('href'); if (h) out.add(h); });

    const all = Array.from(document.querySelectorAll("a[href*='/jogo/futebol/']"));
    all.forEach(a => {
      const h = a.getAttribute('href') || '';
      if (h && !h.includes('/h2h/') && !h.includes('/classificacao/')) out.add(h);
    });
    return Array.from(out);
  });
  const norm = hrefs.map(normalizeDetailUrl).filter(Boolean);
  return Array.from(new Set(norm));
}

// ---------- leitura de cabeçalho/estatísticas ----------
async function readHeaderBasic(page) {
  const getTxt = async (sel) => {
    try {
      const loc = page.locator(sel);
      if ((await loc.count()) === 0) return '';
      const t = await loc.first().textContent();
      return String(t || '').trim();
    } catch { return ''; }
  };

  const home = await getTxt('.duelParticipant__home .participant__participantName a');
  const away = await getTxt('.duelParticipant__away .participant__participantName a');
  const league = await getTxt('.detail__breadcrumbs [data-testid="wcl-breadcrumbs"] li:nth-child(3) span');

  const status =
    (await getTxt('.detailScore__status .fixedHeaderDuel__detailStatus')) ||
    (await getTxt('.fixedScore__status .fixedHeaderDuel__detailStatus')) ||
    (await getTxt('.fixedHeaderDuel__detailStatus'));

  const minuteTxt =
    (await getTxt('.detailScore__status .eventTime')) ||
    (await getTxt('.fixedScore__status .eventTime')) ||
    (await getTxt('.fixedHeaderDuel .eventTime'));

  const minute = (() => {
    const m = parseInt(String(minuteTxt || '').replace(/[^\d]/g, ''), 10);
    return Number.isFinite(m) ? m : null;
  })();

  let goalsHome = null, goalsAway = null;
  try {
    const spans = await page
      .locator('.detailScore__wrapper span:not(.detailScore__divider)')
      .allTextContents();
    const nums = (spans || [])
      .map((s) => parseInt(String(s || '').replace(/[^\d]/g, ''), 10))
      .filter((x) => Number.isFinite(x));
    if (nums.length >= 2) { goalsHome = nums[0]; goalsAway = nums[1]; }
  } catch {}

  return { home, away, league, status, minute, goalsHome, goalsAway };
}

async function readStatRowByLabel(page, label) {
  try {
    const row = page.locator(
      `xpath=//div[@data-testid='wcl-statistics' and .//div[@data-testid='wcl-statistics-category']//strong[contains(normalize-space(.), "${label}")]]`
    );
    if (await row.count()) {
      const hTxt = await row.locator(".wcl-homeValue_3Q-7P [data-testid='wcl-scores-simpleText-01']").first().textContent().catch(()=> '');
      const aTxt = await row.locator(".wcl-awayValue_Y-QR1 [data-testid='wcl-scores-simpleText-01']").first().textContent().catch(()=> '');
      const h = parseInt(String(hTxt || '').replace(/[^\d]/g, ''), 10) || 0;
      const a = parseInt(String(aTxt || '').replace(/[^\d]/g, ''), 10) || 0;
      return { home: h, away: a };
    }
  } catch {}
  return { home: 0, away: 0 };
}

async function readStatRowByAnyLabel(page, labels) {
  for (const lb of labels) {
    const v = await readStatRowByLabel(page, lb);
    if ((v.home || v.away) && (v.home >= 0 || v.away >= 0)) return v;
  }
  return { home: 0, away: 0 };
}

async function readFloatRowByLabel(page, label) {
  try {
    const row = page.locator(
      `xpath=//div[@data-testid='wcl-statistics' and .//div[@data-testid='wcl-statistics-category']//strong[contains(normalize-space(.), "${label}")]]`
    );
    if (await row.count()) {
      const hTxt = await row.locator(".wcl-homeValue_3Q-7P [data-testid='wcl-scores-simpleText-01']").first().textContent().catch(()=> '');
      const aTxt = await row.locator(".wcl-awayValue_Y-QR1 [data-testid='wcl-scores-simpleText-01']").first().textContent().catch(()=> '');
      const toNum = (t) => {
        const s = String(t || '').trim().replace(',', '.').replace(/[^\d.]/g, '');
        const v = parseFloat(s);
        return Number.isFinite(v) ? v : 0;
      };
      return { home: toNum(hTxt), away: toNum(aTxt) };
    }
  } catch {}
  return { home: 0, away: 0 };
}

async function readStatsSnapshot(page) {
  const hasStats = await page.locator("[data-testid='wcl-statistics']").count().catch(()=>0);
  if (!hasStats) return null;

  const shotsTotal = await readStatRowByLabel(page, 'Total de finalizações');
  const shotsOn    = await readStatRowByLabel(page, 'Finalizações no alvo');
  const shotsOff   = await readStatRowByLabel(page, 'Finalizações para fora');
  const shotsBlk   = await readStatRowByLabel(page, 'Finalizações bloqueadas');           // novo
  const bigChances = await readStatRowByAnyLabel(page, ['Chances claras','Grandes oportunidades']); // novo
  const corners    = await readStatRowByLabel(page, 'Escanteios');
  const dangAtt    = await readStatRowByLabel(page, 'Ataques perigosos');
  const xg         = await readFloatRowByLabel(page, 'xG');                                // novo

  return {
    st_home: shotsTotal.home,  st_away: shotsTotal.away,
    sot_home: shotsOn.home,    sot_away: shotsOn.away,
    soff_home: shotsOff.home,  soff_away: shotsOff.away,
    sblk_home: shotsBlk.home,  sblk_away: shotsBlk.away,        // novo
    bc_home:   bigChances.home,bc_away:   bigChances.away,      // novo
    xg_home:   xg.home,        xg_away:   xg.away,              // novo
    corners_home: corners.home, corners_away: corners.away,
    da_home: dangAtt.home,     da_away: dangAtt.away,
  };
}

// ========================= MONITOR =========================
function prettyFirstLine(h) {
  const sh = Number.isFinite(h.goalsHome) ? h.goalsHome : '?';
  const sa = Number.isFinite(h.goalsAway) ? h.goalsAway : '?';
  return `${h.home} (${sh}) x (${sa}) ${h.away} — ${h.league || ''}`;
}

// pressões ponderadas (mantida)
function pressureFromDelta(dh, da) {
  const ph = (dh.sot || 0) * 3 + (dh.soff || 0) * 1.5 + (dh.da || 0) * 0.5 + (dh.corners || 0) * 0.5;
  const pa = (da.sot || 0) * 3 + (da.soff || 0) * 1.5 + (da.da || 0) * 0.5 + (da.corners || 0) * 0.5;
  return { home: +ph.toFixed(2), away: +pa.toFixed(2) };
}

// encontra o tick base pela janela de minutos de jogo (<= currMin - PRESS_WINDOW_MIN)
function findBaselineTickByMinute(ticksAsc, currMinute, winMin) {
  if (!Array.isArray(ticksAsc) || !ticksAsc.length) return null;
  const target = (currMinute ?? 0) - winMin;
  if (!Number.isFinite(target)) return ticksAsc[0];

  let candidate = null;
  for (let i = ticksAsc.length - 1; i >= 0; i--) {
    const t = ticksAsc[i];
    if (t && Number.isFinite(t.minute) && t.minute <= target) { candidate = t; break; }
  }
  // se ainda não achou (início do jogo), usa o primeiro tick registrado
  return candidate || ticksAsc[0];
}

async function monitorMatch(ctx, url, eventId, db) {
  const page = await ctx.newPage();

  // acelera: bloqueia imagens/fontes/mídias
  await page.route('**/*', (route) => {
    const type = route.request().resourceType();
    if (type === 'image' || type === 'media' || type === 'font') return route.abort();
    route.continue();
  });

  // navega e aceita cookies
  try {
    await page.goto(url, { waitUntil: 'commit', timeout: NAV_TIMEOUT_MS });
    await acceptCookiesIfAny(page);
    await page.waitForSelector(
      ".detailScore__status .eventTime, .fixedHeaderDuel .eventTime, .detailScore__wrapper span:not(.detailScore__divider), [data-testid='wcl-statistics']",
      { timeout: FIRST_PAINT_TIMEOUT_MS }
    );
  } catch (e) {
    if (LOG_BASIC) console.log(`[monitor ${eventId}] falha de navegação:`, e.message || e);
    try { await page.close(); } catch {}
    return;
  }

  // espera 2s antes de iniciar leitura
  await page.waitForTimeout(2000);

  // leitura inicial
  const header = await readHeaderBasic(page);
  if (LOG_BASIC) console.log('Log:', prettyFirstLine(header));

  // verifica se há estatísticas; se não, encerra monitor
  const stats0 = await readStatsSnapshot(page);
  if (!stats0) {
    if (LOG_BASIC) console.log(`[monitor ${eventId}] sem estatísticas — encerrando monitor.`);
    try { await page.close(); } catch {}
    return;
  }

  // registra meta do jogo
  try {
    await db.upsertMatch({
      event_id: eventId,
      url,
      league: header.league || '',
      home: header.home || '',
      away: header.away || '',
      created_at: Date.now(),
    });
  } catch {}

  // função de tick (20s)
  let closed = false;
  const doTick = async () => {
    if (closed) return;
    try {
      const h = await readHeaderBasic(page);
      const s = await readStatsSnapshot(page);
      if (!s) return;

      const nowTs = Date.now();

      // pegue o tick anterior (para gols e status)
      const store = require('../db'); // mesma base, conexão do módulo db.js
      const prev = store.getLastTick(eventId);

      // monta o row atual
      const row = {
        event_id: eventId,
        ts: nowTs,
        minute: h.minute ?? null,
        status: h.status || '',
        goals_home: Number.isFinite(h.goalsHome) ? h.goalsHome : null,
        goals_away: Number.isFinite(h.goalsAway) ? h.goalsAway : null,
        st_home: s.st_home, st_away: s.st_away,
        sot_home: s.sot_home, sot_away: s.sot_away,
        soff_home: s.soff_home, soff_away: s.soff_away,
        sblk_home: s.sblk_home, sblk_away: s.sblk_away,   // novo
        bc_home:   s.bc_home,   bc_away:   s.bc_away,     // novo
        xg_home:   s.xg_home,   xg_away:   s.xg_away,     // novo
        da_home: s.da_home, da_away: s.da_away,
        corners_home: s.corners_home, corners_away: s.corners_away,
      };

      // salva tick
      await db.insertTick(row);
      // dentro do doTick(), depois de await db.insertTick(row);
      const goalHalf = require('../engine/goal_half_agent');
      await goalHalf.onTick({
        event_id: eventId,
        league: header.league || '',
        home: header.home || '',
        away: header.away || '',
        minute: row.minute || 0,
        ts: nowTs
      });


      // ---- DELTA NA JANELA DE 6 MIN (ou PRESS_WINDOW_MIN) POR MINUTO DE JOGO ----
      const hist = store.getTicksRange(eventId, 0, nowTs); // ordenado por ts
      const base = findBaselineTickByMinute(hist, row.minute ?? 0, PRESS_WINDOW_MIN) || prev;

      const dH = {
        sot: (row.sot_home || 0) - ((base && base.sot_home) || 0),
        soff: (row.soff_home || 0) - ((base && base.soff_home) || 0),
        da:   (row.da_home   || 0) - ((base && base.da_home)   || 0),
        corners: (row.corners_home || 0) - ((base && base.corners_home) || 0),
      };
      const dA = {
        sot: (row.sot_away || 0) - ((base && base.sot_away) || 0),
        soff: (row.soff_away || 0) - ((base && base.soff_away) || 0),
        da:   (row.da_away   || 0) - ((base && base.da_away)   || 0),
        corners: (row.corners_away || 0) - ((base && base.corners_away) || 0),
      };

      const press = pressureFromDelta(dH, dA);

      // dispara o analisador (ele grava predições e emite entradas se >= 0.90)
      const analyzer = require('../engine/live_analyzer');
      await analyzer.onTickAnalyze({
        event_id: eventId,
        league: header.league || '',
        home: header.home || '',
        away: header.away || '',
        minute: row.minute || 0,
        status: row.status || '',
        goals_home: row.goals_home || 0,
        goals_away: row.goals_away || 0,
        prev_goals_home: prev?.goals_home || 0,
        prev_goals_away: prev?.goals_away || 0,
        // pressão na JANELA:
        press_home: press.home,
        press_away: press.away,
        ts: nowTs
      });

      // encerra se finalizou
      if (/Encerrado|A terminar|Final/i.test(h.status || '')) {
        if (LOG_BASIC) console.log(
          `[ENDED ${new Date().toISOString().replace('T',' ').slice(0,19)}] ${h.home} ${h.goalsHome ?? '?'}-${h.goalsAway ?? '?'} ${h.away}`
        );
        closed = true;
        try { await page.close(); } catch {}
      }
    } catch {
      // segue no próximo tick
    }
  };

  // roda imediato + a cada POLL_MS
  await doTick();
  const int = setInterval(doTick, POLL_MS);
  page.on('close', () => { clearInterval(int); closed = true; });
  return page; // deixa a aba viva monitorando
}

// ========================= SCRAPER MAIN =========================
async function startScraper(options = {}) {
  const listUrl = options.listUrl || LIST_URL;
  const headless = options.headless !== undefined ? !!options.headless : HEADLESS;

  // prepara DB
  ensureDir(path.dirname(DB_PATH));
  const db = makeDb(DB_PATH, JSONL_DIR);

  if (LOG_BASIC) console.log('[scraper] start', { listUrl, headless, PRESS_WINDOW_MIN });

  const browser = await chromium.launch({ headless });

  // contexto mobile para LISTA
  const listCtx = await browser.newContext({
    userAgent: DETAIL_UA,
    viewport: { width: 390, height: 844 },
    deviceScaleFactor: 3,
    isMobile: true,
    hasTouch: true,
  });
  const pageList = await listCtx.newPage();
  await pageList.route('**/*', (route) => {
    const type = route.request().resourceType();
    if (type === 'image' || type === 'media' || type === 'font') return route.abort();
    route.continue();
  });

  // contexto mobile para DETALHES
  const detailCtx = await browser.newContext({
    userAgent: DETAIL_UA,
    viewport: { width: 390, height: 844 },
    deviceScaleFactor: 3,
    isMobile: true,
    hasTouch: true,
  });

  const known = new Set();

  async function discoverAndOpenAll() {
    try {
      if (LOG_BASIC) console.log('[scraper] discovery...');
      await pageList.goto(listUrl, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT_MS }).catch(()=>{});
      await acceptCookiesIfAny(pageList);

      // dá mais tempo para SPA hidratar e tabs renderizarem
      await pageList.waitForLoadState('networkidle', { timeout: 8000 }).catch(()=>{});
      await pageList.waitForSelector('.filters__group .filters__tab', { timeout: FILTER_WAIT_MS }).catch(()=>{});
      await pageList.waitForTimeout(400);

      await ensureLiveFilter(pageList);
      await pageList.waitForTimeout(600);

      const links = await collectLiveLinks(pageList);
      if (LOG_BASIC) console.log(`[scraper] links ao vivo encontrados: ${links.length}`);

      for (const url of links) {
        const id = parseEventIdFromHref(url) || url;
        if (known.has(id)) continue;

        known.add(id);
        await monitorMatch(detailCtx, url, id, db);
        await new Promise(r => setTimeout(r, 200));
      }
    } catch (e) {
      if (LOG_BASIC) console.log('[scraper] discovery error:', e.message || e);
    }
  }

  await discoverAndOpenAll();
  const rediscTimer = setInterval(discoverAndOpenAll, REDISCOVER_MS);

  const shutdown = async () => {
    clearInterval(rediscTimer);
    try { await listCtx.close(); } catch {}
    try { await detailCtx.close(); } catch {}
    try { await browser.close(); } catch {}
    try { db.close(); } catch {}
  };

  process.on('SIGINT',  async () => { await shutdown(); process.exit(0); });
  process.on('SIGTERM', async () => { await shutdown(); process.exit(0); });

  return { close: shutdown };
}

module.exports = { startScraper };
