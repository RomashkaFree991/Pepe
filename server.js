const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const BOT_USERNAME = process.env.BOT_USERNAME || 'GiftPepeRobot';
const BOT_TOKEN = process.env.BOT_TOKEN || '7948801307:AAEVkGlfE4kd0dmgifPZPdQb4FK3vvXrdUc';
const ADMIN_IDS = String(process.env.ADMIN_IDS || '8339935446')
  .split(',')
  .map(s => Number(String(s).trim()))
  .filter(Number.isFinite);
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';

const WAIT_MS = 10000;
const PAUSE_MS = 3000;
const SPEED = 0.00018;

const LEGACY_JSON_DB_FILE = process.env.LEGACY_JSON_DB_FILE || path.join(__dirname, 'giftpepe-db.json');
const DATABASE_URL = String(process.env.DATABASE_URL || 'postgresql://postgres.gqbgwxoyjovjeegdruic:EA8AtrNShwx44F6@aws-1-eu-west-1.pooler.supabase.com:5432/postgres').trim();
const WRITE_JSON_BACKUP = process.env.WRITE_JSON_BACKUP === '1';
// Replace BOT_TOKEN / DATABASE_URL later or move them to environment variables.

let Pool;
try {
  ({ Pool } = require('pg'));
} catch (e) {
  console.error('PostgreSQL driver missing. Install it with: npm i pg');
  throw e;
}

if (!DATABASE_URL) {
  console.error('DATABASE_URL is not set. Add your PostgreSQL connection string in environment variables.');
  process.exit(1);
}

const USE_PG_SSL = String(process.env.PGSSL || process.env.POSTGRES_SSL || '1') !== '0';
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: USE_PG_SSL ? { rejectUnauthorized: false } : undefined,
  max: Math.max(1, num(process.env.PGPOOL_MAX, 10)),
  idleTimeoutMillis: Math.max(1000, num(process.env.PG_IDLE_TIMEOUT_MS, 30000)),
  connectionTimeoutMillis: Math.max(1000, num(process.env.PG_CONNECT_TIMEOUT_MS, 10000))
});

const PG_TABLES = {
  users: 'gp_users',
  inventory: 'gp_inventory',
  crash_rounds: 'gp_crash_rounds',
  crash_bets: 'gp_crash_bets',
  promoCodes: 'gp_promo_codes',
  giftChecks: 'gp_gift_checks',
  topups: 'gp_topups',
  meta: 'gp_meta_state'
};

if (!BOT_TOKEN || BOT_TOKEN === 'PASTE_BOT_TOKEN_HERE') {
  console.warn('Warning: BOT_TOKEN is not set. Set it in environment variables before using payments/auth.');
}

const giftList = [
  { name: 'Amber Adder', img: 'Gifts/Amber Adder.png', price: 475 },
  { name: 'Citrone', img: 'Gifts/Citrone.png', price: 519 },
  { name: 'Amethyst', img: 'Gifts/Amethyst.png', price: 525 },
  { name: 'Anniversary', img: 'Gifts/Anniversary.png', price: 545 },
  { name: 'Albino', img: 'Gifts/Albino.png', price: 550 },
  { name: 'Chuckle Crown', img: 'Gifts/Chuckle Crown.png', price: 559 },
  { name: 'Absinthe', img: 'Gifts/Absinthe.png', price: 575 },
  { name: 'Azurite', img: 'Gifts/Azurite.png', price: 575 },
  { name: 'Be Awesome!', img: 'Gifts/Be Awesome!.png', price: 575 },
  { name: 'Baked Logo', img: 'Gifts/Baked Logo.png', price: 578 },
  { name: 'Backyard', img: 'Gifts/Backyard.png', price: 579 },
  { name: 'Bronze Age', img: 'Gifts/Bronze Age.png', price: 583 },
  { name: 'Carrot Cake', img: 'Gifts/Carrot Cake.png', price: 583 },
  { name: 'Cheesecake', img: 'Gifts/Cheesecake.png', price: 583 },
  { name: 'Choco Chips', img: 'Gifts/Choco Chips.png', price: 583 },
  { name: 'Citrus', img: 'Gifts/Citrus.png', price: 583 },
  { name: 'Citrus Fresh', img: 'Gifts/Citrus Fresh.png', price: 584 },
  { name: 'Court Jester', img: 'Gifts/Court Jester.png', price: 584 },
  { name: 'Dark Cherry', img: 'Gifts/Dark Cherry.png', price: 584 },
  { name: 'Bronze', img: 'Gifts/Bronze.png', price: 585 },
  { name: 'Cyber Ruby', img: 'Gifts/Cyber Ruby.png', price: 585 },
  { name: 'Black Ink', img: 'Gifts/Black Ink.png', price: 599 },
  { name: 'Canceled', img: 'Gifts/Canceled.png', price: 647 },
  { name: 'Adam', img: 'Gifts/Adam.png', price: 650 },
  { name: 'Blue Beam', img: 'Gifts/Blue Beam.png', price: 650 },
  { name: 'Candyman', img: 'Gifts/Candyman.png', price: 667 },
  { name: 'Apple Fresh', img: 'Gifts/Apple Fresh.png', price: 684 },
  { name: 'Alpine Fern', img: 'Gifts/Alpine Fern.png', price: 707 },
  { name: 'Cycloppy', img: 'Gifts/Cycloppy.png', price: 713 },
  { name: 'Angry Ghost', img: 'Gifts/Angry Ghost.png', price: 743 },
  { name: 'Chamomile', img: 'Gifts/Chamomile.png', price: 784 },
  { name: 'Affection', img: 'Gifts/Affection.png', price: 814 },
  { name: 'Arlequin', img: 'Gifts/Arlequin.png', price: 821 },
  { name: 'Anno Domini', img: 'Gifts/Anno Domini.png', price: 825 },
  { name: 'Blue Steel', img: 'Gifts/Blue Steel.png', price: 825 },
  { name: 'April Fools', img: 'Gifts/April Fools.png', price: 833 },
  { name: 'Dark Secret', img: 'Gifts/Dark Secret.png', price: 833 },
  { name: 'Berry Button', img: 'Gifts/Berry Button.png', price: 835 },
  { name: '8 Ball', img: 'Gifts/8 Ball.png', price: 867 },
  { name: 'Brand New', img: 'Gifts/Brand New.png', price: 875 },
  { name: 'Berryllium', img: 'Gifts/Berryllium.png', price: 949 },
  { name: 'Antique', img: 'Gifts/Antique.png', price: 978 },
  { name: 'Barbie', img: 'Gifts/Barbie.png', price: 987 },
  { name: 'Carambola', img: 'Gifts/Carambola.png', price: 1030 },
  { name: 'Cone of Cold', img: 'Gifts/Cone of Cold.png', price: 1054 },
  { name: 'Azalea', img: 'Gifts/Azalea.png', price: 1066 },
  { name: 'Amphibian', img: 'Gifts/Amphibian.png', price: 1069 },
  { name: 'Alpha', img: 'Gifts/Alpha.png', price: 1147 },
  { name: 'Anatomy', img: 'Gifts/Anatomy.png', price: 1173 },
  { name: 'Bronze', img: 'Gifts/Bronze.png', price: 1200 },
  { name: 'Butterfly Tie', img: 'Gifts/Butterfly Tie.png', price: 1248 },
  { name: 'Balloon Face', img: 'Gifts/Balloon Face.png', price: 1250 },
  { name: 'Berry Blaster', img: 'Gifts/Berry Blaster.png', price: 1400 },
  { name: 'Amanita', img: 'Gifts/Amanita.png', price: 1403 },
  { name: 'Butterflight', img: 'Gifts/Butterflight.png', price: 1416 },
  { name: 'Acid Trip', img: 'Gifts/Acid Trip.png', price: 1441 },
  { name: 'Crash Test', img: 'Gifts/Crash Test.png', price: 1498 },
  { name: 'Gouda Wax', img: 'Gifts/Gouda Wax.png', price: 1579 },
  { name: 'Bee Movie', img: 'Gifts/Bee Movie.png', price: 1632 },
  { name: 'Astro Bot', img: 'Gifts/Astro Bot.png', price: 1667 },
  { name: 'Billiard', img: 'Gifts/Billiard.png', price: 1755 },
  { name: 'Afterglow', img: 'Gifts/Afterglow.png', price: 1807 },
  { name: 'Alien Script', img: 'Gifts/Alien Script.png', price: 1888 },
  { name: 'Alchemy', img: 'Gifts/Alchemy.png', price: 2040 },
  { name: 'Bubble Bath', img: 'Gifts/Bubble Bath.png', price: 2333 },
  { name: 'Amortentia', img: 'Gifts/Amortentia.png', price: 2452 },
  { name: 'Abyss Heart', img: 'Gifts/Abyss Heart.png', price: 2454 },
  { name: 'Black Hole', img: 'Gifts/Black Hole.png', price: 2779 },
  { name: 'Airy Souffle', img: 'Gifts/Airy Souffle.png', price: 3186 },
  { name: 'Aqua Gem', img: 'Gifts/Aqua Gem.png', price: 3244 },
  { name: 'Aquaviolet', img: 'Gifts/Aquaviolet.png', price: 3285 },
  { name: 'Disco', img: 'Gifts/Disco.png', price: 3329 },
  { name: 'Candy Cane', img: 'Gifts/Candy Cane.png', price: 3334 },
  { name: '3D Glow', img: 'Gifts/3D Glow.png', price: 3715 },
  { name: 'Creepy Po', img: 'Gifts/Creepy Po.png', price: 4273 },
  { name: 'Banana Pox', img: 'Gifts/Banana Pox.png', price: 5255 },
  { name: 'Cruella', img: 'Gifts/Cruella.png', price: 5567 },
  { name: 'Blue Neon', img: 'Gifts/Blue Neon.png', price: 5582 },
  { name: 'Cheshire Cat', img: 'Gifts/Cheshire Cat.png', price: 6264 },
  { name: 'Baller', img: 'Gifts/Baller.png', price: 6273 },
  { name: 'Frogtart', img: 'Gifts/Frogtart.png', price: 7097 },
  { name: 'Cartoon Sky', img: 'Gifts/Cartoon Sky.png', price: 9277 },
  { name: 'Cartoon', img: 'Gifts/Cartoon.png', price: 10016 },
  { name: 'Mad Magenta', img: 'Gifts/Mad Magenta.png', price: 10024 },
  { name: 'Amazon', img: 'Gifts/Amazon.png', price: 10265 },
  { name: 'Adult Tasks', img: 'Gifts/Adult Tasks.png', price: 10731 },
  { name: 'Aurora Joy', img: 'Gifts/Aurora Joy.png', price: 11419 },
  { name: 'Angry Amethyst', img: 'Gifts/Angry Amethyst.png', price: 11498 },
  { name: 'Brainiac', img: 'Gifts/Brainiac.png', price: 12531 },
  { name: 'Agent Orange', img: 'Gifts/Agent Orange.png', price: 16343 },
  { name: 'Boingo', img: 'Gifts/Boingo.png', price: 18089 },
  { name: 'Creeper', img: 'Gifts/Creeper.png', price: 18806 },
  { name: 'Bogartite', img: 'Gifts/Bogartite.png', price: 24445 },
  { name: 'Academic', img: 'Gifts/Academic.png', price: 24666 },
  { name: 'Barbed', img: 'Gifts/Barbed.png', price: 27471 },
  { name: 'Cozy Warmth', img: 'Gifts/Cozy Warmth.png', price: 39284 },
  { name: 'Brewtoad', img: 'Gifts/Brewtoad.png', price: 41735 },
  { name: 'Ardente', img: 'Gifts/Ardente.png', price: 48395 },
  { name: 'Liberty', img: 'Gifts/Liberty.png', price: 50111 },
  { name: 'Bluebird', img: 'Gifts/Bluebird.png', price: 92573 },
  { name: 'Action Film', img: 'Gifts/Action Film.png', price: 145452 },
];


function nowIso() {
  return new Date().toISOString();
}

function num(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function randomId(prefix = 'id') {
  return prefix + '_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 10);
}

function defaultDb() {
  return {
    users: {},
    inventory: {},
    crash_rounds: {},
    crash_bets: {},
    promoCodes: {},
    giftChecks: {},
    topups: {},
    meta: {
      ids: { inventory: 0, crash_rounds: 0 },
      botUpdateOffset: 0
    }
  };
}

function normalizeDb(parsed) {
  const safe = parsed && typeof parsed === 'object' ? parsed : {};
  return {
    ...defaultDb(),
    ...safe,
    users: safe.users || {},
    inventory: safe.inventory || {},
    crash_rounds: safe.crash_rounds || {},
    crash_bets: safe.crash_bets || {},
    promoCodes: safe.promoCodes || {},
    giftChecks: safe.giftChecks || {},
    topups: safe.topups || {},
    meta: {
      ...defaultDb().meta,
      ...(safe.meta || {}),
      ids: { ...defaultDb().meta.ids, ...((safe.meta || {}).ids || {}) }
    }
  };
}

function clone(obj) {
  return obj == null ? obj : JSON.parse(JSON.stringify(obj));
}

function loadLegacyJsonDb() {
  if (!fs.existsSync(LEGACY_JSON_DB_FILE)) return null;
  try {
    const raw = fs.readFileSync(LEGACY_JSON_DB_FILE, 'utf8');
    return normalizeDb(raw ? JSON.parse(raw) : defaultDb());
  } catch (e) {
    console.error('Failed to load legacy JSON DB:', e);
    return null;
  }
}

async function ensurePgSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${PG_TABLES.users} (
      id TEXT PRIMARY KEY,
      value JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS ${PG_TABLES.inventory} (
      id TEXT PRIMARY KEY,
      value JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS ${PG_TABLES.crash_rounds} (
      id TEXT PRIMARY KEY,
      value JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS ${PG_TABLES.crash_bets} (
      id TEXT PRIMARY KEY,
      value JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS ${PG_TABLES.promoCodes} (
      id TEXT PRIMARY KEY,
      value JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS ${PG_TABLES.giftChecks} (
      id TEXT PRIMARY KEY,
      value JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS ${PG_TABLES.topups} (
      id TEXT PRIMARY KEY,
      value JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS ${PG_TABLES.meta} (
      id TEXT PRIMARY KEY,
      value JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
}

async function loadCollection(tableName) {
  const result = await pool.query(`SELECT id, value FROM ${tableName}`);
  const out = {};
  for (const row of result.rows) {
    out[String(row.id)] = row.value;
  }
  return out;
}

async function loadDbFromPg() {
  try {
    const [users, inventory, crash_rounds, crash_bets, promoCodes, giftChecks, topups, metaRows] = await Promise.all([
      loadCollection(PG_TABLES.users),
      loadCollection(PG_TABLES.inventory),
      loadCollection(PG_TABLES.crash_rounds),
      loadCollection(PG_TABLES.crash_bets),
      loadCollection(PG_TABLES.promoCodes),
      loadCollection(PG_TABLES.giftChecks),
      loadCollection(PG_TABLES.topups),
      loadCollection(PG_TABLES.meta)
    ]);

    const meta = metaRows.main || null;
    const hasAnyData = Object.keys(users).length || Object.keys(inventory).length || Object.keys(crash_rounds).length ||
      Object.keys(crash_bets).length || Object.keys(promoCodes).length || Object.keys(giftChecks).length ||
      Object.keys(topups).length || meta;

    if (!hasAnyData) return null;

    return normalizeDb({ users, inventory, crash_rounds, crash_bets, promoCodes, giftChecks, topups, meta });
  } catch (e) {
    console.error('Failed to load PostgreSQL DB:', e);
    return null;
  }
}

async function syncMapTable(client, tableName, objectMap) {
  const target = objectMap && typeof objectMap === 'object' ? objectMap : {};
  const existingRows = await client.query(`SELECT id FROM ${tableName}`);
  const existingIds = new Set(existingRows.rows.map(row => String(row.id)));

  for (const [id, value] of Object.entries(target)) {
    await client.query(
      `INSERT INTO ${tableName} (id, value, updated_at)
       VALUES ($1, $2::jsonb, NOW())
       ON CONFLICT (id) DO UPDATE SET
         value = EXCLUDED.value,
         updated_at = NOW()`,
      [String(id), JSON.stringify(value)]
    );
    existingIds.delete(String(id));
  }

  if (existingIds.size) {
    await client.query(`DELETE FROM ${tableName} WHERE id = ANY($1::text[])`, [[...existingIds]]);
  }
}

async function saveDbNow(state) {
  const payload = normalizeDb(state || defaultDb());
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await syncMapTable(client, PG_TABLES.users, payload.users);
    await syncMapTable(client, PG_TABLES.inventory, payload.inventory);
    await syncMapTable(client, PG_TABLES.crash_rounds, payload.crash_rounds);
    await syncMapTable(client, PG_TABLES.crash_bets, payload.crash_bets);
    await syncMapTable(client, PG_TABLES.promoCodes, payload.promoCodes);
    await syncMapTable(client, PG_TABLES.giftChecks, payload.giftChecks);
    await syncMapTable(client, PG_TABLES.topups, payload.topups);
    await syncMapTable(client, PG_TABLES.meta, { main: payload.meta });
    await client.query('COMMIT');

    if (WRITE_JSON_BACKUP) {
      fs.writeFileSync(LEGACY_JSON_DB_FILE, JSON.stringify(payload, null, 2));
    }
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('Failed to save PostgreSQL DB:', e);
    throw e;
  } finally {
    client.release();
  }
}

const db = defaultDb();
let saveTimer = null;
let saveChain = Promise.resolve();
let shuttingDown = false;

function queueSaveSnapshot(snapshot) {
  saveChain = saveChain
    .then(() => saveDbNow(snapshot))
    .catch(err => {
      console.error('Save queue error:', err);
    });
  return saveChain;
}

async function initPersistentDb() {
  await ensurePgSchema();

  const remote = await loadDbFromPg();
  if (remote) {
    Object.assign(db, remote);
    return;
  }

  const legacy = loadLegacyJsonDb();
  if (legacy) {
    Object.assign(db, legacy);
    await saveDbNow(db);
    return;
  }

  Object.assign(db, defaultDb());
  await saveDbNow(db);
}

function flushDbNow() {
  clearTimeout(saveTimer);
  return queueSaveSnapshot(clone(db));
}

function saveDbSoon() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    void flushDbNow();
  }, 50);
}

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`Received ${signal}, flushing PostgreSQL state...`);
  try {
    await flushDbNow();
    await saveChain.catch(() => {});
    await pool.end();
  } catch (e) {
    console.error('Shutdown flush failed:', e);
  } finally {
    process.exit(0);
  }
}

process.on('SIGINT', () => {
  void shutdown('SIGINT');
});

process.on('SIGTERM', () => {
  void shutdown('SIGTERM');
});

process.on('beforeExit', () => {
  void flushDbNow();
});

function nextNumericId(table) {
  db.meta.ids[table] = num(db.meta.ids[table], 0) + 1;
  saveDbSoon();
  return db.meta.ids[table];
}

function serializeUser(row, viewer) {
  if (!row) return null;
  const base = {
    id: String(row.telegram_id || ''),
    telegram_id: num(row.telegram_id, 0),
    first_name: row.first_name || '',
    username: row.username || '',
    photo_url: row.photo_url || '',
    topup_total: num(row.topup_total, 0),
    referrals_count: num(row.referrals_count, 0),
    earned_total: num(row.earned_total, 0),
    referrer_telegram_id: row.referrer_telegram_id == null ? null : num(row.referrer_telegram_id, 0),
    created_at: row.created_at || null,
    updated_at: row.updated_at || null
  };
  if (viewer && String(viewer.id) === String(row.telegram_id)) {
    base.balance = num(row.balance, 0);
  }
  return base;
}

function serializeRow(table, row, viewer) {
  if (!row) return null;
  if (table === 'users') return serializeUser(row, viewer);
  if (table === 'crash_bets') return {
    id: String(row.id || (String(row.round_id) + '_' + String(row.user_id))),
    round_id: row.round_id,
    user_id: num(row.user_id, 0),
    user_name: row.user_name || '',
    user_photo: row.user_photo || '',
    amount: num(row.amount, 0),
    status: row.status || 'active',
    cashout_amount: row.cashout_amount == null ? null : num(row.cashout_amount, 0),
    cashout_multiplier: row.cashout_multiplier == null ? null : Number(row.cashout_multiplier),
    reward_claimed: !!row.reward_claimed,
    reward_type: row.reward_type || null,
    created_at: row.created_at || null,
    updated_at: row.updated_at || null
  };
  return { ...clone(row), id: row.id != null ? row.id : row.code != null ? row.code : undefined };
}

function pickFields(row, fields) {
  if (!row) return row;
  if (!fields || fields === '*') return row;
  const names = String(fields).split(',').map(s => s.trim()).filter(Boolean);
  if (!names.length) return row;
  const out = {};
  for (const name of names) {
    if (name === 'id') out.id = row.id;
    else out[name] = row[name];
  }
  return out;
}

function applyFilters(rows, filters = []) {
  return (rows || []).filter(row => {
    for (const filter of filters || []) {
      if (!filter) continue;
      const field = filter.field;
      const rowValue = field === 'id' ? row.id : row[field];
      if (filter.type === 'eq') {
        if (String(rowValue) !== String(filter.value)) return false;
      } else if (filter.type === 'in') {
        const values = Array.isArray(filter.values) ? filter.values.map(v => String(v)) : [];
        if (!values.includes(String(rowValue))) return false;
      }
    }
    return true;
  });
}

function sortRows(rows, orderCfg) {
  if (!orderCfg || !orderCfg.field) return rows;
  const asc = orderCfg.ascending !== false;
  const field = orderCfg.field;
  return rows.slice().sort((a, b) => {
    const av = a[field] ?? '';
    const bv = b[field] ?? '';
    if (av < bv) return asc ? -1 : 1;
    if (av > bv) return asc ? 1 : -1;
    return 0;
  });
}

function tableRows(table, viewer) {
  let rows = [];
  if (table === 'users') rows = Object.values(db.users).map(row => serializeRow('users', row, viewer));
  else if (table === 'inventory') rows = Object.values(db.inventory).map(row => serializeRow('inventory', row, viewer));
  else if (table === 'crash_rounds') rows = Object.values(db.crash_rounds).map(row => serializeRow('crash_rounds', row, viewer));
  else if (table === 'crash_bets') rows = Object.values(db.crash_bets).map(row => serializeRow('crash_bets', row, viewer));
  else if (table === 'promoCodes') rows = Object.values(db.promoCodes).map(row => serializeRow('promoCodes', row, viewer));
  else if (table === 'giftChecks') rows = Object.values(db.giftChecks).map(row => serializeRow('giftChecks', row, viewer));
  else if (table === 'topups') rows = Object.values(db.topups).map(row => serializeRow('topups', row, viewer));
  return rows;
}

function userById(id) {
  return db.users[String(id)] || null;
}

function ensureUserFromTelegram(user, payload = {}) {
  const userId = String(user.id);
  const prev = db.users[userId] || null;
  const now = nowIso();
  const next = {
    telegram_id: num(user.id, 0),
    first_name: payload.first_name != null ? String(payload.first_name) : (user.first_name || prev?.first_name || ''),
    username: payload.username != null ? String(payload.username) : (user.username || prev?.username || ''),
    photo_url: payload.photo_url != null ? String(payload.photo_url) : (user.photo_url || prev?.photo_url || ''),
    balance: num(prev?.balance, 0),
    topup_total: num(prev?.topup_total, 0),
    referrals_count: num(prev?.referrals_count, 0),
    earned_total: num(prev?.earned_total, 0),
    referrer_telegram_id: prev?.referrer_telegram_id ?? null,
    created_at: prev?.created_at || now,
    updated_at: now
  };
  db.users[userId] = next;
  saveDbSoon();
  return next;
}

function deriveGiftForAmount(amount) {
  const safeAmount = Math.max(0, Math.floor(Number(amount) || 0));
  for (let i = giftList.length - 1; i >= 0; i--) {
    if (safeAmount >= giftList[i].price) return { ...giftList[i] };
  }
  return null;
}

function roundCreatedMs(round) {
  return new Date(round.created_at).getTime();
}

function getActualRoundTimes(round) {
  const created = roundCreatedMs(round);
  const target = Math.max(Number(round.target_multiplier || 1.01), 1.01);
  const gameStart = created + WAIT_MS;
  const crashMs = Math.log(target) / SPEED;
  const crashAt = gameStart + crashMs;
  const nextAt = crashAt + PAUSE_MS;
  return { created, target, gameStart, crashMs, crashAt, nextAt };
}

function randomTargetMultiplier() {
  const r = Math.random();
  let min = 1.05, max = 2.20;
  if (r >= 0.50 && r < 0.80) { min = 2.21; max = 5.50; }
  else if (r >= 0.80 && r < 0.94) { min = 5.51; max = 14.50; }
  else if (r >= 0.94 && r < 0.99) { min = 14.51; max = 40.00; }
  else if (r >= 0.99) { min = 40.01; max = 85.00; }
  return Number((Math.random() * (max - min) + min).toFixed(2));
}

function markRoundCrashed(roundId) {
  const round = db.crash_rounds[String(roundId)];
  if (!round || round.status === 'crashed') return round || null;
  const times = getActualRoundTimes(round);
  round.status = 'crashed';
  round.crash_time = new Date(times.crashAt).toISOString();
  round.updated_at = nowIso();
  for (const bet of Object.values(db.crash_bets)) {
    if (String(bet.round_id) === String(roundId) && bet.status === 'active') {
      bet.status = 'lost';
      bet.updated_at = nowIso();
    }
  }
  saveDbSoon();
  return round;
}

function ensureCrashState() {
  const now = Date.now();
  for (const round of Object.values(db.crash_rounds)) {
    if (!round || round.status !== 'active') continue;
    const times = getActualRoundTimes(round);
    if (times.nextAt <= now) markRoundCrashed(round.id);
  }
}

function pickCurrentCrashRound(rows) {
  const items = (rows || []).filter(Boolean).slice().sort((a, b) => roundCreatedMs(a) - roundCreatedMs(b));
  const now = Date.now();
  const live = items.filter(round => getActualRoundTimes(round).nextAt > now - 250);
  return live.length ? live[live.length - 1] : null;
}

function cleanupExtraActiveRounds(keepId) {
  const activeRows = Object.values(db.crash_rounds).filter(r => r && r.status === 'active');
  for (const row of activeRows) {
    if (String(row.id) !== String(keepId)) markRoundCrashed(row.id);
  }
}

function createCrashRound(createdAtMs = Date.now(), user = null, targetMultiplier = randomTargetMultiplier()) {
  const id = nextNumericId('crash_rounds');
  const createdAt = new Date(Math.floor(createdAtMs)).toISOString();
  const round = {
    id,
    status: 'active',
    target_multiplier: Number(targetMultiplier || 1.01),
    created_at: createdAt,
    crash_time: null,
    created_by_telegram_id: num(user?.id, 0),
    updated_at: createdAt
  };
  db.crash_rounds[String(id)] = round;
  saveDbSoon();
  return round;
}

function backfillCrashHistory(minCount = 8) {
  const crashed = Object.values(db.crash_rounds)
    .filter(round => round && round.status === 'crashed')
    .sort((a, b) => roundCreatedMs(a) - roundCreatedMs(b));

  const missing = Math.max(0, minCount - crashed.length);
  if (!missing) return;

  let cursor = Date.now() - missing * (WAIT_MS + PAUSE_MS + 7000) - 4000;

  for (let i = 0; i < missing; i++) {
    const target = randomTargetMultiplier();
    const round = createCrashRound(cursor, { id: 0 }, target);
    const times = getActualRoundTimes(round);
    round.status = 'crashed';
    round.crash_time = new Date(times.crashAt).toISOString();
    round.updated_at = nowIso();
    cursor = times.nextAt + 250;
  }

  saveDbSoon();
}

function maintainCrashRounds() {
  ensureCrashState();
  const activeRows = Object.values(db.crash_rounds).filter(r => r && r.status === 'active');
  const picked = pickCurrentCrashRound(activeRows);
  if (picked) {
    cleanupExtraActiveRounds(picked.id);
    return picked;
  }
  return createCrashRound(Date.now(), { id: 0 });
}

let crashLoopTimer = null;
function startCrashLoop() {
  if (crashLoopTimer) clearInterval(crashLoopTimer);
  backfillCrashHistory(8);
  maintainCrashRounds();
  crashLoopTimer = setInterval(() => {
    try {
      maintainCrashRounds();
    } catch (e) {
      console.error('Crash loop error:', e);
    }
  }, 1000);
}

function getOrCreateActiveCrashRound(user) {
  ensureCrashState();
  const activeRows = Object.values(db.crash_rounds).filter(r => r && r.status === 'active');
  const picked = pickCurrentCrashRound(activeRows);
  if (picked) {
    cleanupExtraActiveRounds(picked.id);
    return serializeRow('crash_rounds', picked, user);
  }
  const round = createCrashRound(Date.now(), user, randomTargetMultiplier());
  return serializeRow('crash_rounds', round, user);
}

function applyReferralOnFirstJoin(userId, referrerIdRaw) {
  const referrerId = referrerIdRaw ? String(referrerIdRaw) : '';
  if (!referrerId || referrerId === String(userId)) return;
  const user = db.users[String(userId)];
  if (!user || user.referrer_telegram_id) return;
  const referrer = db.users[referrerId];
  if (!referrer) return;
  user.referrer_telegram_id = num(referrerId, 0);
  referrer.referrals_count = num(referrer.referrals_count, 0) + 1;
  referrer.updated_at = nowIso();
  user.updated_at = nowIso();
  saveDbSoon();
}

function applyPromoCode(code, viewer) {
  const clean = String(code || '').trim();
  if (!clean) return { ok: false, error: 'not_found' };
  const promo = db.promoCodes[clean];
  if (!promo) return { ok: false, error: 'not_found' };
  if (num(promo.activations_left, 0) <= 0) return { ok: false, error: 'used_up' };
  promo.used_by = Array.isArray(promo.used_by) ? promo.used_by : [];
  if (promo.used_by.map(v => String(v)).includes(String(viewer.id))) return { ok: false, error: 'already_used' };
  const user = ensureUserFromTelegram(viewer, {});
  const stars = Math.max(0, num(promo.stars, 0));
  user.balance = num(user.balance, 0) + stars;
  user.updated_at = nowIso();
  promo.activations_left = Math.max(0, num(promo.activations_left, 0) - 1);
  promo.used_by.push(num(viewer.id, 0));
  promo.updated_at = nowIso();
  saveDbSoon();
  return { ok: true, stars, balance: user.balance };
}

function createInventoryItem(userId, item) {
  const id = nextNumericId('inventory');
  const row = {
    id,
    user_id: num(userId, 0),
    gift_name: String(item.name || ''),
    gift_img: String(item.img || ''),
    gift_price: Math.max(0, num(item.price, 0)),
    created_at: nowIso(),
    updated_at: nowIso()
  };
  db.inventory[String(id)] = row;
  saveDbSoon();
  return serializeRow('inventory', row);
}

function settleCrashRewardSell(viewer, roundId) {
  const key = String(roundId) + '_' + String(viewer.id);
  const bet = db.crash_bets[key];
  if (!bet || bet.status !== 'won') throw new Error('Награда не найдена');
  const user = ensureUserFromTelegram(viewer, {});
  if (bet.reward_claimed) return { ok: true, balance: num(user.balance, 0) };
  const gift = deriveGiftForAmount(bet.cashout_amount);
  if (gift) {
    user.balance = num(user.balance, 0) + num(bet.cashout_amount, 0);
    user.updated_at = nowIso();
    bet.reward_claimed = true;
    bet.reward_type = 'sold';
    bet.updated_at = nowIso();
    saveDbSoon();
  }
  return { ok: true, balance: num(user.balance, 0) };
}

function settleCrashRewardTake(viewer, roundId) {
  const key = String(roundId) + '_' + String(viewer.id);
  const bet = db.crash_bets[key];
  if (!bet || bet.status !== 'won') throw new Error('Награда не найдена');
  if (bet.reward_claimed && bet.reward_item_id) {
    const existing = db.inventory[String(bet.reward_item_id)];
    return { ok: true, item: serializeRow('inventory', existing) };
  }
  const gift = deriveGiftForAmount(bet.cashout_amount);
  if (!gift) throw new Error('Для этой суммы подарок недоступен');
  const item = createInventoryItem(viewer.id, gift);
  bet.reward_claimed = true;
  bet.reward_type = 'gift';
  bet.reward_item_id = item.id;
  bet.updated_at = nowIso();
  saveDbSoon();
  return { ok: true, item };
}

function applyTopupPaid(userId, amount, topupKey) {
  const prev = userById(userId);
  const user = ensureUserFromTelegram({
    id: userId,
    first_name: prev?.first_name || '',
    username: prev?.username || '',
    photo_url: prev?.photo_url || ''
  }, {});
  const topup = db.topups[topupKey];
  if (topup && topup.status === 'paid') return user;
  user.balance = num(user.balance, 0) + num(amount, 0);
  user.topup_total = num(user.topup_total, 0) + num(amount, 0);
  user.updated_at = nowIso();
  const referrerId = user.referrer_telegram_id ? String(user.referrer_telegram_id) : '';
  const bonus = Math.floor(num(amount, 0) * 0.10);
  if (referrerId && bonus > 0 && db.users[referrerId]) {
    db.users[referrerId].balance = num(db.users[referrerId].balance, 0) + bonus;
    db.users[referrerId].earned_total = num(db.users[referrerId].earned_total, 0) + bonus;
    db.users[referrerId].updated_at = nowIso();
  }
  if (topup) {
    topup.status = 'paid';
    topup.updated_at = nowIso();
    topup.paid_at = nowIso();
  }
  saveDbSoon();
  return user;
}

function parseInvoicePayload(payload) {
  const parts = String(payload || '').split('|');
  if (parts.length !== 4 || parts[0] !== 'topup') return null;
  return { userId: parts[1], amount: num(parts[2], 0), topupKey: parts[3] };
}

async function tgApi(method, payload = null) {
  const url = `https://api.telegram.org/bot${BOT_TOKEN}/${method}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload || {})
  });
  const json = await res.json();
  if (!json.ok) throw new Error(json.description || ('Telegram API error in ' + method));
  return json.result;
}

function parseInitData(initData) {
  const params = new URLSearchParams(initData || '');
  const hash = params.get('hash');
  if (!hash) return null;
  params.delete('hash');
  const dataCheckString = Array.from(params.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}=${v}`)
    .join('\n');
  const secret = crypto.createHmac('sha256', 'WebAppData').update(BOT_TOKEN).digest();
  const checkHash = crypto.createHmac('sha256', secret).update(dataCheckString).digest('hex');
  if (checkHash !== hash) return null;
  const authDate = num(params.get('auth_date'), 0);
  if (authDate && (Date.now() / 1000 - authDate > 60 * 60 * 24)) return null;
  try {
    const user = JSON.parse(params.get('user') || 'null');
    return user && user.id ? user : null;
  } catch (_e) {
    return null;
  }
}

function authFromHeaders(headers, body) {
  const initData = headers['x-telegram-init-data'] || body?.initData || '';
  const user = parseInitData(initData);
  if (!user || !user.id) return null;
  return {
    id: num(user.id, 0),
    first_name: user.first_name || '',
    username: user.username || '',
    photo_url: user.photo_url || ''
  };
}

function tableSelect(table, viewer, body) {
  ensureCrashState();
  let rows = tableRows(table, viewer);
  if (table === 'inventory') rows = rows.filter(r => String(r.user_id) === String(viewer.id));
  if (table === 'users') rows = rows.filter(r => String(r.telegram_id) === String(viewer.id));
  if (table === 'topups') rows = rows.filter(r => String(r.user_id) === String(viewer.id));
  if ((table === 'promoCodes' || table === 'giftChecks') && !ADMIN_IDS.map(Number).includes(Number(viewer.id))) rows = [];
  rows = applyFilters(rows, body.filters || []);
  rows = sortRows(rows, body.orderCfg);
  if (body.limitN) rows = rows.slice(0, body.limitN);
  rows = rows.map(row => pickFields(row, body.fields));
  if (body.expect === 'single') return { data: rows[0] || null, error: rows[0] ? null : { message: 'No rows' } };
  if (body.expect === 'maybeSingle') return { data: rows[0] || null, error: null };
  return { data: rows, error: null };
}

function upsertOwnUser(viewer, payload) {
  const user = ensureUserFromTelegram(viewer, {
    first_name: payload.first_name,
    username: payload.username,
    photo_url: payload.photo_url
  });
  return serializeRow('users', user, viewer);
}

function createCrashBet(viewer, payload) {
  ensureCrashState();
  const round = db.crash_rounds[String(payload.round_id)];
  if (!round || round.status !== 'active') throw new Error('Раунд недоступен');
  const times = getActualRoundTimes(round);
  if (Date.now() >= times.gameStart) throw new Error('Ставки закрыты');
  const key = String(payload.round_id) + '_' + String(viewer.id);
  if (db.crash_bets[key]) throw new Error('Ставка уже существует');
  const amount = Math.max(0, Math.floor(num(payload.amount, 0)));
  if (!amount) throw new Error('Некорректная ставка');
  const user = ensureUserFromTelegram(viewer, {});
  if (num(user.balance, 0) < amount) throw new Error('Недостаточно средств');
  user.balance = num(user.balance, 0) - amount;
  user.updated_at = nowIso();
  const bet = {
    id: key,
    round_id: payload.round_id,
    user_id: num(viewer.id, 0),
    user_name: payload.user_name || viewer.first_name || '',
    user_photo: payload.user_photo || viewer.photo_url || '',
    amount,
    status: 'active',
    cashout_amount: null,
    cashout_multiplier: null,
    reward_claimed: false,
    reward_type: null,
    created_at: nowIso(),
    updated_at: nowIso()
  };
  db.crash_bets[key] = bet;
  saveDbSoon();
  return serializeRow('crash_bets', bet, viewer);
}

function cashoutCrashBet(viewer, filters, payload) {
  ensureCrashState();
  const roundId = filters.find(f => f.field === 'round_id')?.value || payload.round_id;
  const key = String(roundId) + '_' + String(viewer.id);
  const bet = db.crash_bets[key];
  if (!bet) throw new Error('Ставка не найдена');
  if (bet.status !== 'active') return serializeRow('crash_bets', bet, viewer);
  const round = db.crash_rounds[String(bet.round_id)];
  if (!round || round.status !== 'active') throw new Error('Раунд завершён');
  const times = getActualRoundTimes(round);
  const now = Date.now();
  if (now < times.gameStart || now >= times.crashAt) throw new Error('Кэшаут уже недоступен');
  bet.status = 'won';
  bet.cashout_multiplier = Number(payload.cashout_multiplier || 1);
  bet.cashout_amount = Math.max(0, Math.floor(num(payload.cashout_amount, 0)));
  bet.updated_at = nowIso();
  const gift = deriveGiftForAmount(bet.cashout_amount);
  if (gift) {
    bet.reward_claimed = false;
    bet.reward_type = 'gift_pending';
  } else {
    const user = ensureUserFromTelegram(viewer, {});
    user.balance = num(user.balance, 0) + num(bet.cashout_amount, 0);
    user.updated_at = nowIso();
    bet.reward_claimed = true;
    bet.reward_type = 'stars';
  }
  saveDbSoon();
  return serializeRow('crash_bets', bet, viewer);
}

function updateCrashRounds(filters, payload) {
  ensureCrashState();
  const rows = applyFilters(tableRows('crash_rounds', null), filters || []);
  for (const row of rows) {
    if (payload.status === 'crashed') markRoundCrashed(row.id);
  }
  saveDbSoon();
}

function deleteInventoryRows(viewer, filters, expect) {
  const rows = applyFilters(tableRows('inventory', viewer).filter(r => String(r.user_id) === String(viewer.id)), filters || []);
  if (!rows.length) return expect === 'single' || expect === 'maybeSingle' ? null : [];
  const user = ensureUserFromTelegram(viewer, {});
  const deleted = [];
  for (const row of rows) {
    const item = db.inventory[String(row.id)];
    if (!item) continue;
    user.balance = num(user.balance, 0) + num(item.gift_price, 0);
    user.updated_at = nowIso();
    deleted.push(serializeRow('inventory', item, viewer));
    delete db.inventory[String(row.id)];
  }
  saveDbSoon();
  if (expect === 'single' || expect === 'maybeSingle') return deleted[0] || null;
  return deleted;
}

function updateCrashBet(viewer, filters, payload) {
  if (payload.status === 'won') return cashoutCrashBet(viewer, filters || [], payload || {});
  const roundId = filters.find(f => f.field === 'round_id')?.value || payload.round_id;
  const key = String(roundId) + '_' + String(viewer.id);
  const bet = db.crash_bets[key];
  if (!bet) return null;
  if (payload.status === 'lost' && bet.status === 'active') {
    bet.status = 'lost';
    bet.updated_at = nowIso();
    saveDbSoon();
  }
  return serializeRow('crash_bets', bet, viewer);
}

async function readBody(req) {
  if (req.method === 'GET' || req.method === 'HEAD') return {};
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', chunk => {
      data += chunk;
      if (data.length > 2 * 1024 * 1024) {
        reject(new Error('Body too large'));
        req.destroy();
      }
    });
    req.on('end', () => {
      if (!data) return resolve({});
      try {
        resolve(JSON.parse(data));
      } catch (_e) {
        resolve({});
      }
    });
    req.on('error', reject);
  });
}

function sendJson(res, status, payload) {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': ALLOWED_ORIGIN,
    'Access-Control-Allow-Headers': 'Content-Type, X-Telegram-Init-Data',
    'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS'
  });
  res.end(JSON.stringify(payload));
}

function sendText(res, status, text, mime = 'text/plain; charset=utf-8') {
  res.writeHead(status, {
    'Content-Type': mime,
    'Access-Control-Allow-Origin': ALLOWED_ORIGIN,
    'Access-Control-Allow-Headers': 'Content-Type, X-Telegram-Init-Data',
    'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS'
  });
  res.end(text);
}

function mimeType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  return {
    '.html': 'text/html; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.svg': 'image/svg+xml',
    '.gif': 'image/gif',
    '.webp': 'image/webp',
    '.ico': 'image/x-icon'
  }[ext] || 'application/octet-stream';
}

function safeFilePath(urlPath) {
  const clean = decodeURIComponent(urlPath.split('?')[0]);
  const base = clean === '/' ? '/index.html' : clean;
  const resolved = path.normalize(path.join(__dirname, base));
  if (!resolved.startsWith(__dirname)) return null;
  return resolved;
}

async function requireAuth(headers, body) {
  const viewer = authFromHeaders(headers, body);
  if (!viewer) return null;
  ensureUserFromTelegram(viewer, {});
  return viewer;
}

function isAdmin(viewer) {
  return ADMIN_IDS.map(Number).includes(Number(viewer.id));
}

function serializeUserForAdmin(row) {
  if (!row) return null;
  return {
    id: String(row.telegram_id || ''),
    telegram_id: num(row.telegram_id, 0),
    first_name: row.first_name || '',
    username: row.username || '',
    photo_url: row.photo_url || '',
    balance: num(row.balance, 0),
    topup_total: num(row.topup_total, 0),
    referrals_count: num(row.referrals_count, 0),
    earned_total: num(row.earned_total, 0),
    referrer_telegram_id: row.referrer_telegram_id == null ? null : num(row.referrer_telegram_id, 0),
    created_at: row.created_at || null,
    updated_at: row.updated_at || null
  };
}

function getAdminOverview() {
  const users = Object.values(db.users || {});
  const inventory = Object.values(db.inventory || {});
  const crashRounds = Object.values(db.crash_rounds || {});
  const crashBets = Object.values(db.crash_bets || {});
  const topups = Object.values(db.topups || {});
  const giftChecks = Object.values(db.giftChecks || {});
  const promoCodes = Object.values(db.promoCodes || {});

  return {
    ok: true,
    stats: {
      users: users.length,
      inventory: inventory.length,
      crash_rounds: crashRounds.length,
      crash_bets: crashBets.length,
      topups: topups.length,
      paid_topups: topups.filter(t => t && t.status === 'paid').length,
      gift_checks: giftChecks.length,
      promo_codes: promoCodes.length,
      total_balance: users.reduce((sum, u) => sum + num(u?.balance, 0), 0),
      total_topup: users.reduce((sum, u) => sum + num(u?.topup_total, 0), 0)
    },
    latest: {
      users: users
        .map(serializeUserForAdmin)
        .sort((a, b) => String(b.updated_at || '').localeCompare(String(a.updated_at || '')))
        .slice(0, 20),
      topups: topups
        .slice()
        .sort((a, b) => String(b.created_at || '').localeCompare(String(a.created_at || '')))
        .slice(0, 20),
      crash_rounds: crashRounds
        .slice()
        .sort((a, b) => Number(b.id || 0) - Number(a.id || 0))
        .slice(0, 20)
    }
  };
}

function getAdminUsersList() {
  return {
    ok: true,
    items: Object.values(db.users || {})
      .map(serializeUserForAdmin)
      .sort((a, b) => {
        const topupDiff = num(b.topup_total, 0) - num(a.topup_total, 0);
        if (topupDiff !== 0) return topupDiff;
        const balanceDiff = num(b.balance, 0) - num(a.balance, 0);
        if (balanceDiff !== 0) return balanceDiff;
        return String(b.updated_at || '').localeCompare(String(a.updated_at || ''));
      })
  };
}

function getAdminUserDetails(uid) {
  const user = db.users[String(uid)];
  if (!user) return { ok: false, error: 'User not found' };

  const inventory = Object.values(db.inventory || {})
    .filter(item => String(item.user_id) === String(uid))
    .sort((a, b) => String(b.created_at || '').localeCompare(String(a.created_at || '')));

  const topups = Object.values(db.topups || {})
    .filter(item => String(item.user_id) === String(uid))
    .sort((a, b) => String(b.created_at || '').localeCompare(String(a.created_at || '')));

  const crashBets = Object.values(db.crash_bets || {})
    .filter(item => String(item.user_id) === String(uid))
    .sort((a, b) => String(b.created_at || '').localeCompare(String(a.created_at || '')));

  const referrals = Object.values(db.users || {})
    .filter(item => String(item.referrer_telegram_id || '') === String(uid))
    .map(serializeUserForAdmin)
    .sort((a, b) => String(b.created_at || '').localeCompare(String(a.created_at || '')));

  return {
    ok: true,
    user: serializeUserForAdmin(user),
    inventory,
    topups,
    crash_bets: crashBets,
    referrals,
    stats: {
      inventory_count: inventory.length,
      referrals_count: referrals.length,
      topups_count: topups.length,
      crash_bets_count: crashBets.length,
      paid_topup_total: topups.filter(t => t && t.status === 'paid').reduce((sum, t) => sum + num(t?.amount, 0), 0)
    }
  };
}

async function handleApi(req, res, pathname, body) {
  const viewer = await requireAuth(req.headers, body);

  if (pathname === '/api/health' && req.method === 'GET') {
    return sendJson(res, 200, { ok: true, bot: BOT_USERNAME, users: Object.keys(db.users).length });
  }

  if (pathname === '/api/auth/session' && req.method === 'POST') {
    if (!viewer) return sendJson(res, 401, { error: 'Unauthorized' });
    const user = ensureUserFromTelegram(viewer, {});
    return sendJson(res, 200, {
      ok: true,
      user: {
        uid: String(viewer.id),
        telegram_id: num(viewer.id, 0),
        first_name: user.first_name || viewer.first_name || '',
        username: user.username || viewer.username || '',
        photo_url: user.photo_url || viewer.photo_url || ''
      }
    });
  }

  if (!viewer) return sendJson(res, 401, { error: 'Unauthorized' });

  if (pathname.startsWith('/api/rpc/') && req.method === 'POST') {
    const name = decodeURIComponent(pathname.slice('/api/rpc/'.length));
    try {
      if (name === 'server_now') return sendJson(res, 200, { data: nowIso(), error: null });
      if (name === 'get_or_create_active_crash_round') return sendJson(res, 200, { data: getOrCreateActiveCrashRound(viewer), error: null });
      if (name === 'init_user_profile') {
        ensureUserFromTelegram(viewer, {
          first_name: body.p_first_name,
          username: body.p_username,
          photo_url: body.p_photo_url
        });
        applyReferralOnFirstJoin(viewer.id, body.p_referrer_telegram_id);
        return sendJson(res, 200, { data: serializeRow('users', db.users[String(viewer.id)], viewer), error: null });
      }
      if (name === 'get_referral_stats') {
        const user = ensureUserFromTelegram(viewer, {});
        return sendJson(res, 200, { data: { referrals_count: num(user.referrals_count, 0), earned_total: num(user.earned_total, 0) }, error: null });
      }
      if (name === 'get_top_toppers') {
        const limitN = Math.max(1, Math.min(num(body.p_limit, 10), 50));
        const rows = Object.values(db.users)
          .map(u => serializeRow('users', u, null))
          .sort((a, b) => num(b.topup_total, 0) - num(a.topup_total, 0))
          .slice(0, limitN)
          .map(r => ({
            telegram_id: r.telegram_id,
            first_name: r.first_name || '',
            username: r.username || '',
            photo_url: r.photo_url || '',
            total_topup: num(r.topup_total, 0)
          }));
        return sendJson(res, 200, { data: rows, error: null });
      }
      if (name === 'get_gift_check_public') {
        const code = String(body.p_code || '').trim();
        const check = db.giftChecks[code];
        if (!check) return sendJson(res, 200, { data: { ok: false, error: 'not_found', amount: 0, status: 'missing' }, error: null });
        return sendJson(res, 200, {
          data: {
            ok: true,
            amount: num(check.amount, 0),
            status: check.status || 'active',
            claimed_by_current_user: String(check.claimed_by_telegram_id || '') === String(viewer.id)
          },
          error: null
        });
      }
      if (name === 'claim_gift_check') {
        const code = String(body.p_code || '').trim();
        const check = db.giftChecks[code];
        if (!check) return sendJson(res, 200, { data: { ok: false, error: 'not_found' }, error: null });
        if ((check.status || 'active') !== 'active') return sendJson(res, 200, { data: { ok: false, error: 'already_claimed', amount: num(check.amount, 0) }, error: null });
        const user = ensureUserFromTelegram(viewer, {});
        user.balance = num(user.balance, 0) + num(check.amount, 0);
        user.updated_at = nowIso();
        check.status = 'claimed';
        check.claimed_by_telegram_id = num(viewer.id, 0);
        check.claimed_by_name = viewer.first_name || '';
        check.claimed_by_username = viewer.username || '';
        check.claimed_by_photo = viewer.photo_url || '';
        check.claimed_at = nowIso();
        saveDbSoon();
        return sendJson(res, 200, { data: { ok: true, amount: num(check.amount, 0), balance: user.balance }, error: null });
      }
      return sendJson(res, 200, { data: null, error: { message: 'Unknown rpc: ' + name } });
    } catch (e) {
      console.error('RPC error:', e);
      return sendJson(res, 200, { data: null, error: { message: e.message || 'RPC failed' } });
    }
  }

  if (pathname.startsWith('/api/table/') && req.method === 'POST') {
    const rest = pathname.slice('/api/table/'.length).split('/');
    const table = decodeURIComponent(rest[0] || '');
    const action = decodeURIComponent(rest[1] || '');
    const filters = Array.isArray(body.filters) ? body.filters : [];
    const payload = body.payload || {};
    try {
      if (action === 'select') return sendJson(res, 200, tableSelect(table, viewer, body));
      if (action === 'insert') {
        if (table === 'users') {
          const data = upsertOwnUser(viewer, payload || {});
          return sendJson(res, 200, { data: body.expect === 'single' || body.expect === 'maybeSingle' ? data : [data], error: null });
        }
        if (table === 'inventory') {
          const data = createInventoryItem(viewer.id, { name: payload.gift_name, img: payload.gift_img, price: payload.gift_price });
          return sendJson(res, 200, { data: body.expect === 'single' || body.expect === 'maybeSingle' ? data : [data], error: null });
        }
        if (table === 'crash_bets') {
          const data = createCrashBet(viewer, payload || {});
          return sendJson(res, 200, { data: body.expect === 'single' || body.expect === 'maybeSingle' ? data : [data], error: null });
        }
        return sendJson(res, 200, { data: null, error: { message: 'Insert not allowed for ' + table } });
      }
      if (action === 'upsert') {
        if (table === 'users') {
          const data = upsertOwnUser(viewer, payload || {});
          return sendJson(res, 200, { data: body.expect === 'single' || body.expect === 'maybeSingle' ? data : [data], error: null });
        }
        if (table === 'crash_bets') {
          const key = String(payload.round_id) + '_' + String(viewer.id);
          if (db.crash_bets[key]) return sendJson(res, 200, { data: serializeRow('crash_bets', db.crash_bets[key], viewer), error: null });
          const data = createCrashBet(viewer, payload || {});
          return sendJson(res, 200, { data: body.expect === 'single' || body.expect === 'maybeSingle' ? data : [data], error: null });
        }
        return sendJson(res, 200, { data: null, error: { message: 'Upsert not allowed for ' + table } });
      }
      if (action === 'update') {
        if (table === 'users') return sendJson(res, 200, { data: upsertOwnUser(viewer, payload || {}), error: null });
        if (table === 'crash_rounds') {
          updateCrashRounds(filters, payload || {});
          return sendJson(res, 200, { data: null, error: null });
        }
        if (table === 'crash_bets') return sendJson(res, 200, { data: updateCrashBet(viewer, filters, payload || {}), error: null });
        return sendJson(res, 200, { data: null, error: { message: 'Update not allowed for ' + table } });
      }
      if (action === 'delete') {
        if (table === 'inventory') return sendJson(res, 200, { data: deleteInventoryRows(viewer, filters, body.expect), error: null });
        return sendJson(res, 200, { data: null, error: { message: 'Delete not allowed for ' + table } });
      }
      return sendJson(res, 200, { data: null, error: { message: 'Unknown action: ' + action } });
    } catch (e) {
      console.error('Table API error:', e);
      return sendJson(res, 200, { data: null, error: { message: e.message || 'Table API failed' } });
    }
  }

  if (pathname === '/api/promo/apply' && req.method === 'POST') {
    try {
      return sendJson(res, 200, applyPromoCode(body.code, viewer));
    } catch (e) {
      return sendJson(res, 400, { error: e.message || 'Promo apply failed' });
    }
  }

  if (pathname === '/api/payments/create-invoice' && req.method === 'POST') {
    try {
      const amount = Math.max(1, Math.floor(num(body.amount, 0)));
      const topupKey = randomId('topup');
      db.topups[topupKey] = {
        id: topupKey,
        user_id: num(viewer.id, 0),
        amount,
        status: 'pending',
        created_at: nowIso(),
        updated_at: nowIso()
      };
      saveDbSoon();
      const invoiceUrl = await tgApi('createInvoiceLink', {
        title: 'GiftPepe — Пополнение',
        description: 'Пополнение баланса на ' + amount + ' ⭐',
        payload: 'topup|' + String(viewer.id) + '|' + String(amount) + '|' + topupKey,
        currency: 'XTR',
        prices: [{ label: amount + ' Stars', amount }]
      });
      db.topups[topupKey].invoice_url = invoiceUrl;
      db.topups[topupKey].status = 'invoice_created';
      db.topups[topupKey].updated_at = nowIso();
      saveDbSoon();
      return sendJson(res, 200, { ok: true, topupKey, invoiceUrl });
    } catch (e) {
      console.error('Create invoice failed:', e);
      return sendJson(res, 400, { error: e.message || 'Не удалось создать счёт' });
    }
  }

  if (pathname === '/api/payments/confirm' && req.method === 'POST') {
    const topupKey = String(body.topupKey || '').trim();
    if (!topupKey || !db.topups[topupKey]) return sendJson(res, 404, { error: 'Topup not found' });
    if (String(db.topups[topupKey].user_id) !== String(viewer.id)) return sendJson(res, 403, { error: 'Forbidden' });
    const started = Date.now();
    let syncAttempted = false;
    while (Date.now() - started < 15000) {
      const topup = db.topups[topupKey];
      if (topup && topup.status === 'paid') {
        const user = ensureUserFromTelegram(viewer, {});
        return sendJson(res, 200, { ok: true, balance: num(user.balance, 0), topup });
      }
      if (!syncAttempted && BOT_TOKEN) {
        syncAttempted = true;
        try {
          await fetchTelegramUpdatesOnce(0);
        } catch (e) {
          console.error('confirm payment sync failed:', e.message || e);
        }
        const syncedTopup = db.topups[topupKey];
        if (syncedTopup && syncedTopup.status === 'paid') {
          const user = ensureUserFromTelegram(viewer, {});
          return sendJson(res, 200, { ok: true, balance: num(user.balance, 0), topup: syncedTopup });
        }
      }
      await new Promise(r => setTimeout(r, 500));
    }
    const user = ensureUserFromTelegram(viewer, {});
    return sendJson(res, 200, { ok: false, balance: num(user.balance, 0), status: db.topups[topupKey]?.status || 'pending' });
  }

  if (pathname === '/api/crash/reward/sell' && req.method === 'POST') {
    try {
      return sendJson(res, 200, settleCrashRewardSell(viewer, body.round_id));
    } catch (e) {
      return sendJson(res, 400, { error: e.message || 'Не удалось продать награду' });
    }
  }

  if (pathname === '/api/crash/reward/take' && req.method === 'POST') {
    try {
      return sendJson(res, 200, settleCrashRewardTake(viewer, body.round_id));
    } catch (e) {
      return sendJson(res, 400, { error: e.message || 'Не удалось получить подарок' });
    }
  }

  if (pathname === '/api/admin/overview' && req.method === 'GET') {
    if (!isAdmin(viewer)) return sendJson(res, 403, { error: 'Admin only' });
    return sendJson(res, 200, getAdminOverview());
  }

  if (pathname === '/api/admin/users' && req.method === 'GET') {
    if (!isAdmin(viewer)) return sendJson(res, 403, { error: 'Admin only' });
    return sendJson(res, 200, getAdminUsersList());
  }

  if (pathname.startsWith('/api/admin/user/') && req.method === 'GET') {
    if (!isAdmin(viewer)) return sendJson(res, 403, { error: 'Admin only' });
    const uid = decodeURIComponent(pathname.slice('/api/admin/user/'.length)).trim();
    if (!uid) return sendJson(res, 400, { error: 'Введите Telegram ID' });
    const payload = getAdminUserDetails(uid);
    return sendJson(res, payload.ok ? 200 : 404, payload);
  }

  if (pathname === '/api/admin/db/export' && req.method === 'GET') {
    if (!isAdmin(viewer)) return sendJson(res, 403, { error: 'Admin only' });
    return sendJson(res, 200, { ok: true, data: clone(db) });
  }

  if (pathname === '/api/admin/promo/create' && req.method === 'POST') {
    if (!isAdmin(viewer)) return sendJson(res, 403, { error: 'Admin only' });
    const code = String(body.code || '').trim();
    const stars = Math.max(1, Math.floor(num(body.stars, 0)));
    const acts = Math.max(1, Math.floor(num(body.activations_left, 1)));
    if (!code) return sendJson(res, 400, { error: 'Введите код' });
    if (db.promoCodes[code]) return sendJson(res, 400, { error: 'Промокод уже существует' });
    db.promoCodes[code] = { code, stars, activations_left: acts, used_by: [], created_by: num(viewer.id, 0), created_at: nowIso(), updated_at: nowIso() };
    saveDbSoon();
    return sendJson(res, 200, { ok: true });
  }

  if (pathname === '/api/admin/promo/list' && req.method === 'GET') {
    if (!isAdmin(viewer)) return sendJson(res, 403, { error: 'Admin only' });
    const items = Object.values(db.promoCodes).sort((a, b) => String(b.created_at || '').localeCompare(String(a.created_at || '')));
    return sendJson(res, 200, { ok: true, items });
  }

  if (pathname.startsWith('/api/admin/promo/') && req.method === 'DELETE') {
    if (!isAdmin(viewer)) return sendJson(res, 403, { error: 'Admin only' });
    const code = decodeURIComponent(pathname.slice('/api/admin/promo/'.length));
    delete db.promoCodes[code];
    saveDbSoon();
    return sendJson(res, 200, { ok: true });
  }

  if (pathname === '/api/admin/give-stars' && req.method === 'POST') {
    if (!isAdmin(viewer)) return sendJson(res, 403, { error: 'Admin only' });
    const uid = String(body.uid || '').trim();
    const stars = Math.max(1, Math.floor(num(body.stars, 0)));
    if (!uid) return sendJson(res, 400, { error: 'Введите Telegram ID' });
    const user = db.users[uid] || {
      telegram_id: num(uid, 0),
      first_name: '',
      username: '',
      photo_url: '',
      balance: 0,
      topup_total: 0,
      referrals_count: 0,
      earned_total: 0,
      referrer_telegram_id: null,
      created_at: nowIso(),
      updated_at: nowIso()
    };
    user.balance = num(user.balance, 0) + stars;
    user.updated_at = nowIso();
    db.users[uid] = user;
    saveDbSoon();
    return sendJson(res, 200, { ok: true, balance: num(user.balance, 0) });
  }

  return sendJson(res, 404, { error: 'Not found' });
}

async function handleRequest(req, res) {
  if (req.method === 'OPTIONS') return sendJson(res, 200, { ok: true });
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = url.pathname;

  if (pathname.startsWith('/api/')) {
    try {
      const body = await readBody(req);
      return await handleApi(req, res, pathname, body);
    } catch (e) {
      console.error('Request error:', e);
      return sendJson(res, 500, { error: e.message || 'Server error' });
    }
  }

  const filePath = safeFilePath(pathname);
  if (!filePath) return sendText(res, 403, 'Forbidden');
  const fallback = path.join(__dirname, 'index.html');
  const target = fs.existsSync(filePath) && fs.statSync(filePath).isFile() ? filePath : fallback;
  if (!fs.existsSync(target)) return sendText(res, 404, 'index.html not found');
  try {
    const data = fs.readFileSync(target);
    return sendText(res, 200, data, mimeType(target));
  } catch (e) {
    return sendText(res, 500, 'Failed to read file');
  }
}

async function fetchTelegramUpdatesOnce(timeoutSec = 25) {
  if (!BOT_TOKEN) return false;
  const offset = num(db.meta.botUpdateOffset, 0);
  const url = new URL(`https://api.telegram.org/bot${BOT_TOKEN}/getUpdates`);
  url.searchParams.set('timeout', String(Math.max(0, Math.floor(timeoutSec || 0))));
  url.searchParams.set('offset', String(offset));
  url.searchParams.set('allowed_updates', JSON.stringify(['pre_checkout_query', 'message']));
  const res = await fetch(url.toString());
  const json = await res.json();
  let changed = false;
  if (json.ok && Array.isArray(json.result)) {
    for (const upd of json.result) {
      db.meta.botUpdateOffset = num(upd.update_id, 0) + 1;
      if (upd.pre_checkout_query) {
        try {
          await tgApi('answerPreCheckoutQuery', { pre_checkout_query_id: upd.pre_checkout_query.id, ok: true });
        } catch (e) {
          console.error('answerPreCheckoutQuery failed:', e);
        }
      }
      const success = upd.message?.successful_payment;
      if (success?.invoice_payload) {
        const parsed = parseInvoicePayload(success.invoice_payload);
        if (parsed && parsed.topupKey && db.topups[parsed.topupKey]) {
          applyTopupPaid(parsed.userId, parsed.amount, parsed.topupKey);
          changed = true;
        }
      }
    }
    saveDbSoon();
    return changed || json.result.length > 0;
  }
  if (json.description) {
    console.error('getUpdates failed:', json.description);
  }
  return false;
}

async function pollTelegramUpdates() {
  while (true) {
    try {
      await fetchTelegramUpdatesOnce(25);
    } catch (e) {
      console.error('Telegram polling error:', e.message || e);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

const server = http.createServer((req, res) => {
  handleRequest(req, res).catch(err => {
    console.error('Unhandled error:', err);
    sendJson(res, 500, { error: 'Server error' });
  });
});

async function start() {
  try {
    await initPersistentDb();
    startCrashLoop();

    server.listen(PORT, () => {
      console.log(`GiftPepe backend listening on http://localhost:${PORT}`);
      pollTelegramUpdates().catch(err => console.error('Polling stopped:', err));
    });
  } catch (err) {
    console.error('Startup failed:', err);
    process.exit(1);
  }
}

start();
