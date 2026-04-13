require('dotenv').config();
const express  = require('express');
const axios    = require('axios');
const path     = require('path');
const fs       = require('fs');
const crypto   = require('crypto');
const Anthropic = require('@anthropic-ai/sdk');
const xmlrpc   = require('xmlrpc');
const twilio   = require('twilio');

const app = express();
app.use(express.json({ limit: '25mb' }));
const PORT = process.env.PORT || 3001;

// ── Directorios de datos ──────────────────────────────────────────
// Datos compartidos con ml-panel (solo lectura, excepto token que también refresca)
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', 'ml-panel', 'data');
// Datos propios de ml-atencion
const OWN_DATA_DIR = path.join(__dirname, 'data');

// ── Sesiones (definidas temprano para que requireToken pueda usarlas) ──
const sessions = new Map();
function getSession(token) {
  const s = sessions.get(token);
  if (!s) return null;
  if (s.expiresAt < Date.now()) { sessions.delete(token); return null; }
  return s;
}

const { CLIENT_ID, CLIENT_SECRET, ANTHROPIC_API_KEY } = process.env;
const REDIRECT_URI = (process.env.REDIRECT_URI || '').trim();
const anthropic = ANTHROPIC_API_KEY ? new Anthropic({ apiKey: ANTHROPIC_API_KEY }) : null;
const ML_API_URL  = 'https://api.mercadolibre.com';
const ML_AUTH_URL = 'https://auth.mercadolibre.com.uy';

let tokenData    = null;
let cachedClaims = [];

// ── SoyDelivery config ──
const SD_API_ID = process.env.SD_API_ID;
const SD_API_KEY = process.env.SD_API_KEY;
const SD_NEGOCIO_ID = parseInt(process.env.SD_NEGOCIO_ID) || 0;
const SD_NEGOCIO_CLAVE = parseInt(process.env.SD_NEGOCIO_CLAVE) || 0;
const SD_API_URL = 'https://soydelivery.com.uy/rest';
const SD_MAP_FILE = path.join(OWN_DATA_DIR, 'sd_mapping.json');

// Mapeo shipment_id → soydelivery_id
let sdMapping = {};
try { if (fs.existsSync(SD_MAP_FILE)) sdMapping = JSON.parse(fs.readFileSync(SD_MAP_FILE, 'utf8')); } catch {}
function saveSdMapping() {
  try { fs.writeFileSync(SD_MAP_FILE, JSON.stringify(sdMapping, null, 2)); } catch {}
}

let sdToken = null;
let sdTokenExpiry = 0;
async function getSdToken() {
  if (sdToken && Date.now() < sdTokenExpiry) return sdToken;
  if (!SD_API_ID || !SD_API_KEY) return null;
  try {
    const r = await axios.post(`${SD_API_URL}/sdws_autenticar`, { ApiId: parseInt(SD_API_ID), ApiKey: SD_API_KEY });
    if (r.data.AccessToken) {
      sdToken = r.data.AccessToken;
      sdTokenExpiry = Date.now() + 14 * 60 * 1000; // 14 min (expira en 15)
      return sdToken;
    }
  } catch(e) { console.error('[soydelivery] auth error:', e.message); }
  return null;
}

async function consultarSoyDelivery(pedidoId) {
  const token = await getSdToken();
  if (!token) return null;
  try {
    const r = await axios.post(`${SD_API_URL}/awsconsultarpedido1`, {
      Negocio_id: SD_NEGOCIO_ID,
      Negocio_clave: SD_NEGOCIO_CLAVE,
      Pedido_id: pedidoId
    }, { headers: { Authorization: `Bearer ${token}` } });
    if (r.data.Error_code === 0) return r.data;
  } catch {}
  return null;
}

async function consultarSoyDeliveryHistorial(shipmentId) {
  const token = await getSdToken();
  if (!token) return null;
  try {
    const r = await axios.post(`${SD_API_URL}/awsconsultapedidohistorial`, {
      Negocio_id: SD_NEGOCIO_ID,
      Negocio_clave: SD_NEGOCIO_CLAVE,
      PedidoExternalId: String(shipmentId)
    }, { headers: { Authorization: `Bearer ${token}`, 'User-Agent': 'SoyDelivery' } });
    if (r.data.Error_code === 200) return r.data;
  } catch {}
  return null;
}

// ── DAC config ──
const DAC_LOGIN = process.env.DAC_LOGIN;
const DAC_PASS = process.env.DAC_PASS;
const DAC_API_URL = 'https://altis-ws.grupoagencia.com:444/JAgencia/JAgencia.asmx';

let dacSession = null;
let dacSessionExpiry = 0;
async function getDacSession() {
  if (dacSession && Date.now() < dacSessionExpiry) return dacSession;
  if (!DAC_LOGIN || !DAC_PASS) return null;
  try {
    const r = await axios.post(`${DAC_API_URL}/wsLogin`, { Login: DAC_LOGIN, Contrasenia: DAC_PASS });
    if (r.data.result === 0 && r.data.data?.[0]) {
      dacSession = r.data.data[0].ID_Session;
      dacSessionExpiry = Date.now() + 55 * 60 * 1000;
      return dacSession;
    }
  } catch(e) { console.error('[dac] login error:', e.message); }
  return null;
}

async function consultarDAC(referencia) {
  const session = await getDacSession();
  if (!session) return null;
  try {
    const r = await axios.post(`${DAC_API_URL}/wsRastreoGuia`, {
      K_Oficina_Origen: '',
      K_Guia: '',
      Referencia: referencia,
      ID_Sesion: session
    });
    if (r.data.result === 0) return r.data;
  } catch {}
  return null;
}

// ── Deri (Robert) config ──
const DERI_API_KEY = process.env.DERI_API_KEY;
const DERI_API_URL = 'https://api.deriapp.com/hub/deri/v1';
const deriHeaders = DERI_API_KEY ? { 'api-key': DERI_API_KEY } : {};

async function consultarDeriOrder(orderId) {
  if (!DERI_API_KEY) return null;
  try {
    const r = await axios.get(`${DERI_API_URL}/orders/${orderId}`, { headers: deriHeaders });
    return r.data.data || r.data;
  } catch {}
  return null;
}

async function consultarDeriStatuses(orderId) {
  if (!DERI_API_KEY) return null;
  try {
    const r = await axios.get(`${DERI_API_URL}/orders/${orderId}/statuses`, { headers: deriHeaders });
    return r.data.data || r.data;
  } catch {}
  return null;
}

async function buscarDeriOrders(params) {
  if (!DERI_API_KEY) return [];
  try {
    const r = await axios.get(`${DERI_API_URL}/orders`, { headers: deriHeaders, params });
    return r.data.data?.items || [];
  } catch {}
  return [];
}

// ── Persistencia del token ML ──
const OWN_TOKEN_FILE = path.join(OWN_DATA_DIR, 'ml_token.json');
const SHARED_TOKEN_FILE = path.join(DATA_DIR, 'ml_token.json');
// Prefiere token propio, fallback al compartido
const TOKEN_FILE = fs.existsSync(OWN_TOKEN_FILE) ? OWN_TOKEN_FILE : (fs.existsSync(SHARED_TOKEN_FILE) ? SHARED_TOKEN_FILE : OWN_TOKEN_FILE);
function saveToken(data) {
  try { fs.writeFileSync(OWN_TOKEN_FILE, JSON.stringify(data), 'utf8'); } catch(e) {}
}
function loadToken() {
  try {
    if (fs.existsSync(TOKEN_FILE)) {
      const t = JSON.parse(fs.readFileSync(TOKEN_FILE, 'utf8'));
      if (t?.access_token) { tokenData = t; console.log('[token] cargado desde disco'); }
    }
  } catch(e) {}
}
loadToken();

// ── Cache de publicaciones (READ-ONLY desde stock_cache.json compartido) ──
let cachedItems   = [];
const STOCK_FILE  = path.join(DATA_DIR, 'stock_cache.json');

function loadStockFromDisk() {
  try {
    if (fs.existsSync(STOCK_FILE)) {
      const saved = JSON.parse(fs.readFileSync(STOCK_FILE, 'utf8'));
      cachedItems = saved.items || [];
      console.log(`[stock] cache cargado desde disco: ${cachedItems.length} publicaciones`);
    }
  } catch(e) { console.error('[stock] error leyendo cache:', e.message); }
}
loadStockFromDisk();

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Refrescar token ML ────────────────────────────────────────────
async function refreshMLToken() {
  if (!tokenData?.refresh_token) return;
  try {
    const r = await axios.post(`${ML_API_URL}/oauth/token`, {
      grant_type: 'refresh_token',
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      refresh_token: tokenData.refresh_token,
    });
    tokenData = r.data;
    saveToken(tokenData);
    console.log('[token] ML token refrescado OK');
  } catch(e) {
    console.error('[token] error refrescando ML token:', e.response?.data || e.message);
  }
}
setInterval(refreshMLToken, 5 * 60 * 60 * 1000);
// Refrescar al arrancar si ya hay token guardado
if (tokenData?.refresh_token) refreshMLToken();

// ── OAuth ML (login independiente) ───────────────────────────────
let pkceVerifier = null;
function generateCodeVerifier() {
  return crypto.randomBytes(32).toString('base64url');
}
function generateCodeChallenge(verifier) {
  return crypto.createHash('sha256').update(verifier).digest('base64url');
}

app.use(express.static(path.join(__dirname, 'public')));

app.get('/login', (req, res) => {
  pkceVerifier = generateCodeVerifier();
  const challenge = generateCodeChallenge(pkceVerifier);
  const authUrl =
    `${ML_AUTH_URL}/authorization` +
    `?response_type=code` +
    `&client_id=${CLIENT_ID}` +
    `&redirect_uri=${encodeURIComponent(REDIRECT_URI)}` +
    `&scope=read_orders+offline_access` +
    `&code_challenge=${challenge}` +
    `&code_challenge_method=S256`;
  res.redirect(authUrl);
});

app.get('/callback', async (req, res) => {
  const { code } = req.query;
  if (!code) return res.status(400).json({ error: 'Falta el parámetro code' });
  try {
    const response = await axios.post(`${ML_API_URL}/oauth/token`, {
      grant_type: 'authorization_code',
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      code,
      redirect_uri: REDIRECT_URI,
      code_verifier: pkceVerifier,
    });
    pkceVerifier = null;
    tokenData = response.data;
    saveToken(tokenData);
    console.log('[oauth] Token obtenido OK');
    res.redirect('/');
  } catch (err) {
    res.status(500).json({ error: 'Error al obtener el token', detail: err.response?.data || err.message });
  }
});

// ── Middleware ────────────────────────────────────────────────────
function requireToken(req, res, next) {
  const sessionTok = req.headers['x-session-token'] || req.query._token;
  if (sessionTok && !getSession(sessionTok)) {
    return res.status(401).json({ error: 'Sesión inválida' });
  }
  if (!tokenData?.access_token) {
    return res.status(401).json({ error: 'No autenticado con ML' });
  }
  next();
}

app.get('/api/auth-status', (req, res) => {
  res.json({ authenticated: !!tokenData?.access_token });
});

// ── Archivos de datos propios ─────────────────────────────────────
const USERS_FILE = path.join(OWN_DATA_DIR, 'users.json');

function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(password, salt, 10000, 64, 'sha256').toString('hex');
}
function loadUsers() {
  try {
    if (fs.existsSync(USERS_FILE)) return JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
  } catch(e) {}
  return [];
}
function saveUsers(data) {
  fs.writeFileSync(USERS_FILE, JSON.stringify(data, null, 2), 'utf8');
}

// Crear usuarios por defecto si no hay usuarios
(function ensureDefaultUsers() {
  const users = loadUsers();
  if (!users.length) {
    const defaults = [
      { username: 'admin', name: 'Administrador', role: 'admin', pass: 'admin123' },
      { username: 'mateo', name: 'Mateo', role: 'admin', pass: 'mateo123' },
    ];
    for (const d of defaults) {
      const salt = crypto.randomBytes(16).toString('hex');
      users.push({
        id:       crypto.randomUUID(),
        username: d.username,
        name:     d.name,
        role:     d.role,
        salt,
        hash:     hashPassword(d.pass, salt),
        createdAt: new Date().toISOString(),
      });
    }
    saveUsers(users);
    console.log('[usuarios] Usuarios por defecto creados (admin/admin123, mateo/mateo123)');
  } else if (!users.find(u => u.username === 'mateo')) {
    const salt = crypto.randomBytes(16).toString('hex');
    users.push({
      id:       crypto.randomUUID(),
      username: 'mateo',
      name:     'Mateo',
      role:     'admin',
      salt,
      hash:     hashPassword('mateo123', salt),
      createdAt: new Date().toISOString(),
    });
    saveUsers(users);
    console.log('[usuarios] Usuario mateo agregado');
  }
})();

function createSession(userId) {
  const token = crypto.randomBytes(32).toString('hex');
  sessions.set(token, { userId, expiresAt: Date.now() + 7 * 24 * 60 * 60 * 1000 });
  return token;
}
function requireUser(req, res, next) {
  const token = req.headers['x-session-token'] || req.query._token;
  const session = token ? getSession(token) : null;
  if (!session) return res.status(401).json({ error: 'Sesión inválida' });
  const users = loadUsers();
  const user = users.find(u => u.id === session.userId);
  if (!user) return res.status(401).json({ error: 'Usuario no encontrado' });
  req.user = user;
  next();
}
function requireAdmin(req, res, next) {
  requireUser(req, res, () => {
    if (req.user.role !== 'admin') return res.status(403).json({ error: 'Sin permiso' });
    next();
  });
}

// ── Auth routes ───────────────────────────────────────────────────
app.post('/api/auth/login', (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: 'Faltan credenciales' });
  const users = loadUsers();
  const user  = users.find(u => u.username === username);
  if (!user) return res.status(401).json({ error: 'Usuario o contraseña incorrectos' });
  const h = hashPassword(password, user.salt);
  if (h !== user.hash) return res.status(401).json({ error: 'Usuario o contraseña incorrectos' });
  const token = createSession(user.id);
  res.json({ token, user: { id: user.id, username: user.username, name: user.name, role: user.role } });
});

app.post('/api/auth/logout', (req, res) => {
  const token = req.headers['x-session-token'];
  if (token) sessions.delete(token);
  res.json({ ok: true });
});

app.get('/api/auth/me', requireUser, (req, res) => {
  const { id, username, name, role } = req.user;
  res.json({ id, username, name, role });
});

// ── User CRUD (admin) ─────────────────────────────────────────────
app.get('/api/users', requireAdmin, (req, res) => {
  res.json(loadUsers().map(({ id, username, name, role, createdAt }) => ({ id, username, name, role, createdAt })));
});

app.post('/api/users', requireAdmin, (req, res) => {
  const { username, name, password, role } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: 'username y password son requeridos' });
  const users = loadUsers();
  if (users.find(u => u.username === username)) return res.status(409).json({ error: 'El usuario ya existe' });
  const salt = crypto.randomBytes(16).toString('hex');
  const user = { id: crypto.randomUUID(), username, name: name || username, role: role || 'user', salt, hash: hashPassword(password, salt), createdAt: new Date().toISOString() };
  users.push(user);
  saveUsers(users);
  res.json({ id: user.id, username: user.username, name: user.name, role: user.role });
});

app.put('/api/users/:id', requireAdmin, (req, res) => {
  const users = loadUsers();
  const idx   = users.findIndex(u => u.id === req.params.id);
  if (idx < 0) return res.status(404).json({ error: 'Usuario no encontrado' });
  const { name, role, password } = req.body || {};
  if (name)     users[idx].name = name;
  if (role)     users[idx].role = role;
  if (password) {
    const salt = crypto.randomBytes(16).toString('hex');
    users[idx].salt = salt;
    users[idx].hash = hashPassword(password, salt);
  }
  saveUsers(users);
  res.json({ id: users[idx].id, username: users[idx].username, name: users[idx].name, role: users[idx].role });
});

app.delete('/api/users/:id', requireAdmin, (req, res) => {
  const users = loadUsers();
  if (users.find(u => u.id === req.params.id)?.role === 'admin' &&
      users.filter(u => u.role === 'admin').length === 1)
    return res.status(400).json({ error: 'No podés eliminar el único admin' });
  saveUsers(users.filter(u => u.id !== req.params.id));
  res.json({ ok: true });
});

// ── POST /notifications — webhook de MercadoLibre (claims) ───────
app.post('/notifications', async (req, res) => {
  res.sendStatus(200);
  const { topic, resource } = req.body || {};
  if (!resource || !tokenData?.access_token) return;
  if (topic !== 'claims' && topic !== 'claims_actions') return;
  const claimId = String(resource).split('/').pop();
  if (!claimId || isNaN(claimId)) return;
  try {
    const r = await axios.get(`${ML_API_URL}/post-purchase/v1/claims/${claimId}`, {
      headers: { Authorization: `Bearer ${tokenData.access_token}` },
    });
    const claim = r.data;
    const idx = cachedClaims.findIndex(c => c.id === claim.id);
    if (idx >= 0) cachedClaims[idx] = claim;
    else cachedClaims.unshift(claim);
    console.log(`[claim] ${topic} — ID ${claimId} guardado (total: ${cachedClaims.length})`);
  } catch (e) {
    console.error(`[claim] Error al obtener claim ${claimId}:`, e.response?.data || e.message);
  }
});

// ── GET /api/reclamos ─────────────────────────────────────────────
app.get('/api/reclamos', requireToken, (req, res) => {
  const offset = parseInt(req.query.offset) || 0;
  const limit  = parseInt(req.query.limit)  || 50;
  const status = req.query.status || '';
  let claims = cachedClaims;
  if (status) claims = claims.filter(c => c.status === status);
  res.json({
    data: claims.slice(offset, offset + limit),
    paging: { total: claims.length, offset, limit },
  });
});

app.get('/api/reclamos/stats', requireToken, (req, res) => {
  const byStatus = {};
  const byType   = {};
  const byReason = {};
  cachedClaims.forEach(c => {
    byStatus[c.status] = (byStatus[c.status] || 0) + 1;
    byType[c.type]     = (byType[c.type]     || 0) + 1;
    if (c.resolution?.reason) byReason[c.resolution.reason] = (byReason[c.resolution.reason] || 0) + 1;
  });
  res.json({ total: cachedClaims.length, by_status: byStatus, by_type: byType, by_reason: byReason });
});

let scanState = { running: false, done: false, checked: 0, total: 0, found: 0, error: null };

async function runClaimsScan(months = 3) {
  if (scanState.running) return;
  scanState = { running: true, done: false, checked: 0, total: 0, found: 0, error: null };
  const headers = { Authorization: `Bearer ${tokenData.access_token}` };
  const from = new Date();
  from.setMonth(from.getMonth() - months);
  const fromStr = from.toISOString().slice(0, 19) + '.000-00:00';
  try {
    const first = await axios.get(`${ML_API_URL}/orders/search`, {
      headers, params: { seller: tokenData.user_id, 'order.status': 'cancelled', 'date_created.from': fromStr, limit: 1 },
    });
    scanState.total = first.data.paging?.total || 0;
    let offset = 0;
    while (offset < scanState.total) {
      const r = await axios.get(`${ML_API_URL}/orders/search`, {
        headers, params: { seller: tokenData.user_id, 'order.status': 'cancelled', 'date_created.from': fromStr, offset, limit: 50 },
      });
      const orders = r.data.results || [];
      await Promise.all(orders.map(async o => {
        try {
          const cr = await axios.get(`${ML_API_URL}/post-purchase/v1/claims/search`, {
            headers, params: { resource_id: o.id, resource: 'order', limit: 10 },
          });
          const claims = cr.data.data || [];
          claims.forEach(claim => {
            const exists = cachedClaims.find(c => c.id === claim.id);
            if (!exists) { cachedClaims.push(claim); scanState.found++; }
          });
        } catch {}
        scanState.checked++;
      }));
      offset += 50;
      await sleep(200);
    }
    cachedClaims.sort((a, b) => new Date(b.date_created) - new Date(a.date_created));
  } catch (e) {
    scanState.error = e.message;
  }
  scanState.running = false;
  scanState.done = true;
}

app.get('/api/reclamos/scan', requireToken, (req, res) => {
  const months = parseInt(req.query.months) || 3;
  if (!scanState.running) runClaimsScan(months);
  res.json({ started: true, state: scanState });
});

app.get('/api/reclamos/scan/status', requireToken, (req, res) => {
  res.json(scanState);
});

// ── GET /api/tareas ───────────────────────────────────────────────
app.get('/api/tareas', requireToken, async (req, res) => {
  const headers = { Authorization: `Bearer ${tokenData.access_token}` };
  const uid = tokenData.user_id;
  try {
    // 1. Preguntas sin responder de los últimos 7 días
    const since7d = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    const qRes = await axios.get(`${ML_API_URL}/questions/search`, {
      headers,
      params: { seller_id: uid, status: 'UNANSWERED', sort_fields: 'date_created', sort_types: 'DESC', limit: 50 },
    });
    const allQuestions = qRes.data.questions || [];
    const recentQuestions = allQuestions.filter(q => q.date_created >= since7d);
    const rawQuestions = recentQuestions.map(q => ({
      id:           q.id,
      item_id:      q.item_id,
      text:         q.text,
      date_created: q.date_created,
      from_id:      q.from?.id || null,
    }));

    // Generar respuestas sugeridas con Claude
    console.log(`[tareas] anthropic=${!!anthropic} rawQuestions=${rawQuestions.length}`);
    let suggestions = {};
    if (anthropic && rawQuestions.length > 0) {
      try {
        const reglasPromptTareas = reglasTexto(filtrarReglasPorContexto(loadReglasNegocio(), 'tareas'));
        const prompt = `Sos el asistente de MUNDO SHOP, una tienda en MercadoLibre Uruguay.
Generá respuestas cortas y amigables a estas preguntas de compradores.
El estilo es: empezar con "Hola, ¿cómo estás?" y terminar con "Agradecemos te hayas comunicado, quedamos a las órdenes! MUNDO SHOP".${reglasPromptTareas}
Respondé SOLO con un JSON válido: un objeto donde cada clave es el id de la pregunta y el valor es la respuesta sugerida.

Preguntas:
${rawQuestions.map(q => `ID ${q.id}: "${q.text}"`).join('\n')}`;

        const r = await anthropic.messages.create({
          model: 'claude-haiku-4-5',
          max_tokens: 4096,
          messages: [{ role: 'user', content: prompt }],
        });
        const text = r.content.find(b => b.type === 'text')?.text || '{}';
        try { fs.writeFileSync(path.join(OWN_DATA_DIR, 'debug_suggestions.json'), JSON.stringify({ text }, null, 2)); } catch {}
        const jsonMatch = text.match(/\{[\s\S]*\}/);
        if (jsonMatch) suggestions = JSON.parse(jsonMatch[0]);
      } catch(e) {
        console.error('[tareas] sugerencias error:', e.message);
      }
    }

    const questions = rawQuestions.map(q => ({ ...q, suggestion: suggestions[q.id] || null }));

    // 2. Últimas 50 órdenes pagadas
    const ordRes = await axios.get(`${ML_API_URL}/orders/search`, {
      headers,
      params: { seller: uid, limit: 50, sort: 'date_desc', order_status: 'paid' },
    });
    const orders = ordRes.data.results || [];

    // 3. Acuerda con comprador: tag no_shipping
    const acordadas = orders.filter(o => (o.tags || []).includes('no_shipping'));

    // 4. Fetch shipments para todas las órdenes con shipping.id
    const withShipping = orders.filter(o => o.shipping?.id && !(o.tags || []).includes('no_shipping'));
    const shipmentDetails = {};
    await Promise.all(withShipping.map(async o => {
      try {
        const r = await axios.get(`${ML_API_URL}/shipments/${o.shipping.id}`, { headers });
        shipmentDetails[o.shipping.id] = r.data;
      } catch(e) { /* skip */ }
    }));
    const me1Orders = withShipping.filter(o => shipmentDetails[o.shipping.id]?.mode === 'me1');
    const dacOrders = withShipping.filter(o => {
      const shp = shipmentDetails[o.shipping.id];
      if (!shp) return false;
      const mode = shp.mode || '';
      const logistic = shp.logistic_type || '';
      return mode !== 'me1' && (mode === 'custom' || logistic === 'dac' || logistic === 'self_service' || logistic === 'drop_off' || mode === 'me2');
    });

    const formatOrder = (order, label) => {
      const oi = order.order_items?.[0] || {};
      const item = oi.item || {};
      const shp = shipmentDetails[order.shipping?.id] || {};
      const addr = shp.receiver_address || {};
      const shipping_address = shp.id ? {
        id: shp.id,
        status: shp.status || '',
        mode: shp.mode || '',
        logistic_type: shp.logistic_type || '',
        receiver_name: addr.receiver_name || order.buyer?.nickname || '',
        address: addr.address_line || '',
        city: addr.city?.name || '',
        state: addr.state?.name || '',
        zip: addr.zip_code || '',
        comment: addr.comment || ''
      } : null;
      return {
        order_id:        order.id,
        date_created:    order.date_created,
        buyer_name:      order.buyer?.nickname || order.buyer?.first_name || '—',
        total_amount:    order.total_amount,
        item_title:      item.title || '—',
        item_thumbnail:  item.thumbnail || null,
        item_sku:        item.seller_sku || (item.attributes || []).find(a => a.id === 'SELLER_SKU')?.value_name || null,
        quantity:        oi.quantity || 1,
        shipping_label:  label,
        shipping_address
      };
    };

    console.log(`[tareas] ${questions.length} preguntas sin responder | ${acordadas.length} acuerda | ${me1Orders.length} ME1 | ${dacOrders.length} DAC`);

    res.json({
      questions,
      acuerda_orders: acordadas.map(o => formatOrder(o, 'Acuerda c/ comprador')),
      me1_orders:     me1Orders.map(o => formatOrder(o, 'ME1')),
      dac_orders:     dacOrders.map(o => formatOrder(o, 'ME1 a coordinar')),
    });
  } catch(err) {
    console.error('[tareas] error:', err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data || err.message });
  }
});

// ── GET /api/devoluciones ─────────────────────────────────────────
app.get('/api/devoluciones', requireToken, async (req, res) => {
  const headers = { Authorization: `Bearer ${tokenData.access_token}` };
  const statusFilter = req.query.status || null;
  try {
    const allClaims = [];
    let offset = 0;
    while (true) {
      const params = { role: 'seller', offset, limit: 50 };
      if (statusFilter && statusFilter !== 'all') params.status = statusFilter;
      const r = await axios.get(`${ML_API_URL}/post-purchase/v1/claims/search`, { headers, params });
      const results = r.data.data || r.data.results || [];
      const total   = r.data.paging?.total ?? results.length;
      for (const c of results) allClaims.push(c);
      if (results.length < 50 || allClaims.length >= total) break;
      offset += 50;
      await sleep(150);
    }
    const orderIds = [...new Set(allClaims.map(c => c.resource_id).filter(Boolean))];
    const orderMap = {};
    for (let i = 0; i < orderIds.length && i < 200; i += 20) {
      const batch = orderIds.slice(i, i + 20);
      await Promise.all(batch.map(async (oid) => {
        try {
          const or = await axios.get(`${ML_API_URL}/orders/${oid}`, { headers });
          const o  = or.data;
          const oi = o.order_items?.[0] || {};
          const it = oi.item || {};
          const sh = o.shipping || {};
          orderMap[oid] = {
            buyer_nickname: o.buyer?.nickname || null,
            buyer_name:     [o.buyer?.first_name, o.buyer?.last_name].filter(Boolean).join(' ') || null,
            logistic_type:  sh.logistic_type  || null,
            shipping_id:    sh.id             || null,
            item_id:        it.id             || null,
            item_title:     it.title          || null,
            item_thumbnail: it.thumbnail      || null,
            item_sku:       it.seller_sku || oi.seller_sku || null,
            variation_name: it.variation_attributes?.map(a => `${a.name}: ${a.value_name}`).join(', ') || null,
            unit_price:     oi.unit_price     || null,
            quantity:       oi.quantity       || null,
          };
        } catch { /* skip */ }
      }));
      if (i + 20 < orderIds.length) await sleep(150);
    }
    const enriched = allClaims.map(c => ({ ...c, ...(orderMap[c.resource_id] || {}) }));
    const returns = enriched.filter(c => c.type === 'return');
    const claims  = enriched.filter(c => c.type !== 'return');
    const by_substatus = {};
    for (const r of returns) {
      const k = r.sub_status || r.substatus || r.status || 'other';
      by_substatus[k] = (by_substatus[k] || 0) + 1;
    }
    const by_status = { opened: 0, closed: 0, resolved: 0 };
    for (const c of enriched) {
      if (c.status === 'opened')        by_status.opened++;
      else if (c.status === 'closed')   by_status.closed++;
      else if (c.status === 'resolved') by_status.resolved++;
    }
    res.json({ returns, claims, by_status, by_substatus, total: enriched.length });
  } catch(err) {
    console.error('[devoluciones] error:', err.response?.data || err.message);
    res.status(500).json({ error: err.response?.data || err.message });
  }
});

// ── Odoo helpers ─────────────────────────────────────────────────
const ODOO_HOST    = process.env.ODOO_HOST;
const ODOO_DB      = process.env.ODOO_DB;
const ODOO_USER    = process.env.ODOO_USER;
const ODOO_API_KEY = process.env.ODOO_API_KEY;

function odooCall(path, method, params) {
  return new Promise((resolve, reject) => {
    const client = xmlrpc.createSecureClient({ host: ODOO_HOST, path });
    client.methodCall(method, params, (err, val) => err ? reject(err) : resolve(val));
  });
}

async function odooAuth() {
  return odooCall('/xmlrpc/2/common', 'authenticate', [ODOO_DB, ODOO_USER, ODOO_API_KEY, {}]);
}

// ── POST /webhook/whatsapp ────────────────────────────────────────
const TWILIO_SID   = process.env.TWILIO_SID;
const TWILIO_TOKEN = process.env.TWILIO_TOKEN;

app.post('/webhook/whatsapp', express.urlencoded({ extended: false }), async (req, res) => {
  const twiml = new twilio.twiml.MessagingResponse();
  const reply = text => {
    twiml.message(text);
    res.type('text/xml').send(twiml.toString());
  };
  try {
    const numMedia = parseInt(req.body.NumMedia || '0');
    const from     = req.body.From;
    if (!numMedia) {
      return reply('Hola! Mandame una foto del pedido escrito a mano y lo cargo en Odoo automáticamente 📋');
    }
    const mediaUrl  = req.body.MediaUrl0;
    const mediaType = req.body.MediaContentType0 || 'image/jpeg';
    const twilioClient = twilio(TWILIO_SID, TWILIO_TOKEN);
    const messageSid   = req.body.MessageSid || req.body.SmsSid;
    const mediaItems   = await twilioClient.messages(messageSid).media.list({ limit: 1 });
    if (!mediaItems.length) return reply('No se encontró imagen en el mensaje.');
    const mediaUri  = mediaItems[0].uri.replace('.json', '');
    const directUrl = `https://api.twilio.com${mediaUri}`;
    const imgResp = await axios.get(directUrl, {
      auth: { username: TWILIO_SID, password: TWILIO_TOKEN },
      responseType: 'arraybuffer',
    });
    const image_base64 = Buffer.from(imgResp.data).toString('base64');
    const msg = await anthropic.messages.create({
      model: 'claude-opus-4-6',
      max_tokens: 1024,
      messages: [{
        role: 'user',
        content: [
          { type: 'image', source: { type: 'base64', media_type: mediaType, data: image_base64 } },
          { type: 'text', text: `Analizá este pedido escrito a mano y extraé el cliente y los productos.

Reglas para variantes de SKU (formato {BASE}-{COLOR} o {BASE}-{COLOR}-{TEMP}):
- Colores: B/BL/Blanco=BLA, N/NG/Negro=NEG, G/GR/Gris=GRI, R/RO/Rosa=ROS, V/VE/Verde=VER, D/DO/Dorado=DOR, P/PL/Plateado=PLA
- Temperatura: F/FRI=FRI, C/CAL=CAL
- "18110: 2B-2N" → dos líneas: {sku:"18110-BLA",cantidad:2} y {sku:"18110-NEG",cantidad:2}
- "22306 3FRI-2CAL" → dos líneas: {sku:"22306-FRI",cantidad:3} y {sku:"22306-CAL",cantidad:2}
- "25203 2BLA-FRI" → una línea: {sku:"25203-BLA-FRI",cantidad:2}
- Si no hay variante, usá el SKU base tal cual

Respondé ÚNICAMENTE con JSON válido, sin texto adicional:
{
  "cliente": "nombre del cliente",
  "productos": [
    { "sku": "18110-BLA", "cantidad": 2 },
    { "sku": "18110-NEG", "cantidad": 2 }
  ]
}` },
        ],
      }],
    });
    let parsed;
    try {
      const text = msg.content[0].text.trim().replace(/^```json\n?/, '').replace(/\n?```$/, '');
      parsed = JSON.parse(text);
    } catch {
      return reply('No pude leer el pedido de la imagen. ¿Podés mandar una foto más clara?');
    }
    const uid = await odooAuth();
    const partners = await odooCall('/xmlrpc/2/object', 'execute_kw', [
      ODOO_DB, uid, ODOO_API_KEY, 'res.partner', 'search_read',
      [[['name', 'ilike', parsed.cliente], ['customer_rank', '>', 0]]],
      { fields: ['id', 'name'], limit: 1 },
    ]);
    if (!partners.length) {
      return reply(`No encontré el cliente "${parsed.cliente}" en Odoo. Verificá el nombre y volvé a intentar.`);
    }
    const partner = partners[0];
    const skus = parsed.productos.map(p => p.sku);
    const products = await odooCall('/xmlrpc/2/object', 'execute_kw', [
      ODOO_DB, uid, ODOO_API_KEY, 'product.product', 'search_read',
      [[['default_code', 'in', skus], ['active', '=', true]]],
      { fields: ['id', 'name', 'default_code', 'lst_price', 'uom_id', 'taxes_id'] },
    ]);
    const productMap = {};
    for (const p of products) productMap[p.default_code] = p;
    const notFound = skus.filter(s => !productMap[s]);
    if (notFound.length) {
      return reply(`No encontré estos SKUs en Odoo: ${notFound.join(', ')}. Revisá y volvé a intentar.`);
    }
    const order_lines = parsed.productos.map(p => {
      const prod = productMap[p.sku];
      return [0, 0, {
        product_id:      prod.id,
        name:            `[${p.sku}] ${prod.name}`,
        product_uom_qty: p.cantidad,
        price_unit:      prod.lst_price,
        product_uom:     prod.uom_id[0],
        tax_id:          [[6, 0, prod.taxes_id || []]],
      }];
    });
    const orderId = await odooCall('/xmlrpc/2/object', 'execute_kw', [
      ODOO_DB, uid, ODOO_API_KEY, 'sale.order', 'create',
      [{
        partner_id:   partner.id,
        pricelist_id: 2,
        date_order:   new Date().toISOString().replace('T', ' ').slice(0, 19),
        order_line:   order_lines,
      }],
    ]);
    const [order] = await odooCall('/xmlrpc/2/object', 'execute_kw', [
      ODOO_DB, uid, ODOO_API_KEY, 'sale.order', 'read',
      [[orderId]], { fields: ['name', 'amount_total'] },
    ]);
    const totalUnits = parsed.productos.reduce((s, p) => s + p.cantidad, 0);
    reply(`✅ Cotización creada en Odoo!\n\n📋 ${order.name}\n👤 ${partner.name}\n📦 ${parsed.productos.length} productos (${totalUnits} unidades)\n💰 $${order.amount_total.toLocaleString('es-UY')}`);
    console.log(`[whatsapp] cotización ${order.name} creada desde ${from}`);
  } catch (err) {
    console.error('[whatsapp] error:', err.message);
    reply('Ocurrió un error procesando el pedido. Intentá de nuevo.');
  }
});

// ── Mensajes post-venta ───────────────────────────────────────────
const MENSAJES_CACHE_FILE = path.join(OWN_DATA_DIR, 'mensajes_cache.json');
const MENSAJES_CACHE_TTL = 5 * 60 * 1000; // 5 minutos
let mensajesCache = null; // { ts, threads }

function loadMensajesCache() {
  try {
    if (fs.existsSync(MENSAJES_CACHE_FILE)) {
      mensajesCache = JSON.parse(fs.readFileSync(MENSAJES_CACHE_FILE, 'utf8'));
    }
  } catch {}
}
function saveMensajesCache(threads) {
  mensajesCache = { ts: Date.now(), threads };
  try { fs.writeFileSync(MENSAJES_CACHE_FILE, JSON.stringify(mensajesCache)); } catch {}
}
loadMensajesCache();

// GET /api/ml/mensajes/pendientes
app.get('/api/ml/mensajes/pendientes', requireToken, async (req, res) => {
  const headers = { Authorization: `Bearer ${tokenData.access_token}` };
  const uid = tokenData.user_id;
  const dias = parseInt(req.query.dias) || 14;
  const force = req.query.force === '1';

  if (!force && mensajesCache && (Date.now() - mensajesCache.ts) < MENSAJES_CACHE_TTL) {
    return res.json({ threads: mensajesCache.threads, total: mensajesCache.threads.length, cached: true });
  }

  try {
    const cutoff = new Date(Date.now() - dias * 24 * 60 * 60 * 1000);
    const allOrders = [];
    const seenPacks = new Set();

    const firstPage = await axios.get(`${ML_API_URL}/orders/search`, {
      headers, params: { seller: uid, limit: 50, sort: 'date_desc', offset: 0 }
    });
    const firstResults = firstPage.data.results || [];

    let neededPages = 1;
    if (firstResults.length && new Date(firstResults[firstResults.length - 1].date_created) >= cutoff) {
      const totalOrders = firstPage.data.paging?.total || 0;
      neededPages = Math.min(20, Math.ceil(totalOrders / 50));
    }

    const pagePromises = [Promise.resolve(firstPage)];
    for (let p = 1; p < neededPages; p++) {
      pagePromises.push(axios.get(`${ML_API_URL}/orders/search`, {
        headers, params: { seller: uid, limit: 50, sort: 'date_desc', offset: p * 50 }
      }).catch(() => ({ data: { results: [] } })));
    }
    const pages = await Promise.all(pagePromises);

    for (const page of pages) {
      for (const o of page.data.results || []) {
        if (new Date(o.date_created) < cutoff) break;
        const key = o.pack_id || o.id;
        if (!seenPacks.has(key)) { seenPacks.add(key); allOrders.push(o); }
      }
    }

    const orders = allOrders;
    const itemMap = {};
    cachedItems.forEach(i => { itemMap[i.id] = i; });

    const threads = [];
    const BATCH = 30;
    for (let i = 0; i < orders.length; i += BATCH) {
      await Promise.all(orders.slice(i, i + BATCH).map(async (order) => {
        const packOrOrder = order.pack_id || order.id;
        try {
          const mr = await axios.get(`${ML_API_URL}/messages/packs/${packOrOrder}/sellers/${uid}`, {
            headers, params: { tag: 'post_sale', limit: 50 }
          });
          const msgs = (mr.data.messages || []).filter(m => m.text && m.text.trim());
          if (!msgs.length) return;

          msgs.sort((a, b) => new Date(a.message_date.created) - new Date(b.message_date.created));
          const lastMsg = msgs[msgs.length - 1];
          const lastIsFromBuyer = lastMsg.from?.user_id !== uid;
          const lastDate = new Date(lastMsg.message_date.created);

          const hasUnreadFromBuyer = msgs.some(m => m.from?.user_id !== uid && !m.message_date?.read);
          if (!lastIsFromBuyer && !hasUnreadFromBuyer) return;
          if (lastDate < cutoff && !hasUnreadFromBuyer) return;

          const oi = order.order_items?.[0] || {};
          const itemId = oi.item?.id;
          const cachedItem = itemMap[itemId] || {};

          let itemTitle = oi.item?.title || cachedItem.title || '';
          let itemThumbnail = cachedItem.thumbnail || '';
          if (itemId && !itemThumbnail) {
            const ctx = await fetchItemContext(itemId).catch(() => null);
            if (ctx) {
              if (!itemTitle) itemTitle = ctx.title || '';
              const fresh = itemMap[itemId];
              if (fresh?.thumbnail) itemThumbnail = fresh.thumbnail;
              else if (ctx.thumbnail) itemThumbnail = ctx.thumbnail;
            }
          }

          const shp = order.shipping || {};
          const addr = shp.receiver_address || {};
          const shipping = shp.id ? {
            id: shp.id,
            status: shp.status || '',
            receiver_name: addr.receiver_name || order.buyer?.nickname || '',
            address: addr.address_line || '',
            city: addr.city?.name || '',
            state: addr.state?.name || '',
            zip: addr.zip_code || '',
            comments: addr.comment || ''
          } : null;

          threads.push({
            order_id: order.id,
            pack_id: packOrOrder,
            buyer_id: order.buyer?.id,
            buyer_name: order.buyer?.nickname || '—',
            item_id: itemId,
            item_title: itemTitle || '—',
            item_thumbnail: itemThumbnail || '',
            order_status: order.status,
            total_amount: order.total_amount,
            shipping,
            last_message: lastMsg.text,
            last_message_date: lastMsg.message_date.created,
            unread: !lastMsg.message_date.read,
            messages: msgs.map(m => ({
              id: m.id,
              from_buyer: m.from?.user_id !== uid,
              text: m.text,
              date: m.message_date.created,
              read: !!m.message_date.read
            }))
          });
        } catch(e) { /* skip */ }
      }));
    }

    threads.sort((a, b) => new Date(b.last_message_date) - new Date(a.last_message_date));
    saveMensajesCache(threads);
    res.json({ threads, total: threads.length, cached: false });
  } catch(e) {
    console.error('[mensajes/pendientes]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/ml/mensajes/simular
app.post('/api/ml/mensajes/simular', requireToken, async (req, res) => {
  if (!anthropic) return res.status(400).json({ error: 'ANTHROPIC_API_KEY no configurada' });
  const { pack_id, item_id, item_title, order_status, messages, buyer_name } = req.body;
  if (!messages?.length) return res.status(400).json({ error: 'messages requerido' });

  // Detectar si la conversación ya cerró (última palabra del comprador es un cierre)
  const lastBuyerText = [...messages].reverse().find(m => m.from_buyer)?.text?.trim().toLowerCase() || '';
  const cierres = ['gracias', 'gracias!', 'gracias!!', 'muchas gracias', 'ok', 'okey', 'dale', 'listo', 'perfecto', 'buenísimo', 'buenisimo', 'de acuerdo', 'entendido', 'ya', 'genial', 'excelente', '👍', '🙏', '✅', 'confirmado', 'recibido'];
  const conversacionCerrada = cierres.some(c => lastBuyerText === c || lastBuyerText === c + '.' || lastBuyerText === c + '!');
  if (conversacionCerrada) {
    return res.json({ respuesta: '¡Con gusto! Quedamos a las órdenes para lo que necesites 😊 MUNDO SHOP', accion: null, cierre: true });
  }

  try {
    let kb = null;
    if (fs.existsSync(QA_KB_FILE)) kb = JSON.parse(fs.readFileSync(QA_KB_FILE, 'utf8'));

    const kbText = kb ? `Estilo MUNDO SHOP:
- Saludo: "${kb.estilo.saludo}"
- Despedida: "${kb.estilo.despedida}"
- Tono: ${kb.estilo.tono}
Reglas:
${kb.reglas_generales.slice(0, 8).map(r => '- ' + r).join('\n')}` : '';

    const reglasText = reglasTexto(filtrarReglasPorContexto(loadReglasNegocio(), 'post-venta'));

    let malasText = '';
    try {
      if (fs.existsSync(BAD_RESP_FILE)) {
        const malas = JSON.parse(fs.readFileSync(BAD_RESP_FILE, 'utf8')).slice(-10);
        if (malas.length) malasText = `\nEJEMPLOS DE RESPUESTAS MALAS — NO imites esto:\n${malas.map(m => `- "${m.respuesta_mala.slice(0, 120)}"${m.motivo ? ' (' + m.motivo + ')' : ''}`).join('\n')}`;
      }
    } catch(_) {}

    const itemCtx = item_id ? await fetchItemContext(item_id) : null;
    const itemText = itemCtx ? buildItemContextText(itemCtx) : `Producto: ${item_title || 'no especificado'}`;

    const historial = messages.map(m =>
      `[${m.from_buyer ? 'COMPRADOR' : 'VENDEDOR'}]: ${m.text}`
    ).join('\n');

    const vendedorYaHabló = messages.slice(0, -1).some(m => !m.from_buyer);
    const lastBuyerMsg = [...messages].reverse().find(m => m.from_buyer);

    // Detectar contexto emocional del último mensaje
    const lastText = lastBuyerMsg?.text?.toLowerCase() || '';
    const esReclamo = /problema|roto|rota|no funciona|no llegó|no llego|reclamo|devolución|devolucion|mal estado|defecto|falla|faltó|falto|incompleto/.test(lastText);
    const esAgradecimiento = /gracias|genial|perfecto|excelente|buenísimo|buenisimo|re bien|muy bien/.test(lastText);

    // Nombre para el saludo
    const nombreComprador = buyer_name ? buyer_name.split(' ')[0] : null;
    const saludoPersonalizado = nombreComprador ? `¡Hola ${nombreComprador}!` : '¡Hola!';

    const r = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 400,
      messages: [{
        role: 'user',
        content: `Sos el equipo de atención al cliente de MUNDO SHOP en Mercado Libre Uruguay.
Tu objetivo es sonar como una persona real: cercana, amigable y profesional al mismo tiempo.
Usá lenguaje natural del Río de la Plata (vos, te, etc). Evitá frases corporativas o robóticas.
${kbText}
${reglasText ? 'REGLAS DEL NEGOCIO (tienen prioridad, usá estos datos exactos):' + reglasText : ''}
${malasText}

${itemText}
Estado de la orden: ${order_status || 'desconocido'}

--- HISTORIAL ---
${historial}
--- FIN ---

ÚLTIMO MENSAJE DEL COMPRADOR: "${lastBuyerMsg?.text || ''}"

INSTRUCCIONES:
${vendedorYaHabló
  ? '- Conversación en curso: NO uses saludo ni despedida. Respondé directo, breve (1-2 oraciones), como si fuera un chat.'
  : `- Primera interacción: Saludá con "${saludoPersonalizado}" y cerrá con "¡Cualquier cosa nos avisás! MUNDO SHOP"`
}
${esReclamo ? '- El cliente tiene un problema: empezá reconociendo el inconveniente con empatía antes de dar la solución.' : ''}
${esAgradecimiento ? '- El cliente está conforme: respondé breve y cálido, no hagas un párrafo largo.' : ''}
- NUNCA uses "Agradecemos te hayas comunicado" ni frases similares
- NUNCA menciones "MUNDO SHOP" más de una vez (solo al cerrar)
- Si no tenés la info exacta, no la inventes
${esReclamo ? '- Sugerí una acción concreta (coordinar retiro, reenviar producto, emitir reembolso, etc)' : ''}
Respondé en JSON: {"respuesta":"...","accion":null}`
      }]
    });

    const text = r.content[0].text.trim();
    try {
      const match = text.match(/\{[\s\S]*\}/);
      const parsed = JSON.parse(match[0]);
      if (typeof parsed.respuesta === 'string' && parsed.respuesta.trim().startsWith('{')) {
        try {
          const inner = JSON.parse(parsed.respuesta);
          if (inner.respuesta) { parsed.respuesta = inner.respuesta; if (!parsed.accion) parsed.accion = inner.accion; }
        } catch(_) {}
      }
      res.json(parsed);
    } catch(e) {
      const clean = text.replace(/```json?/gi,'').replace(/```/g,'').trim();
      res.json({ respuesta: clean, accion: null });
    }
  } catch(e) {
    console.error('[mensajes/simular]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/ml/mensajes/responder
app.post('/api/ml/mensajes/responder', requireToken, async (req, res) => {
  const { pack_id, buyer_id, text } = req.body;
  if (!pack_id || !buyer_id || !text) return res.status(400).json({ error: 'pack_id, buyer_id y text requeridos' });
  const uid = tokenData.user_id;
  try {
    const r = await axios.post(
      `${ML_API_URL}/messages/packs/${pack_id}/sellers/${uid}?tag=post_sale`,
      { from: { user_id: uid }, to: { user_id: buyer_id }, text },
      { headers: { Authorization: `Bearer ${tokenData.access_token}`, 'Content-Type': 'application/json' } }
    );
    res.json({ ok: true, message_id: r.data.id });
  } catch(e) {
    const detail = e.response?.data || e.message;
    console.error('[mensajes/responder]', detail);
    res.status(e.response?.status || 500).json({ error: detail });
  }
});

// ── Preguntas frecuentes por publicación ─────────────────────────
const PREGUNTAS_FILE      = path.join(OWN_DATA_DIR, 'preguntas_por_publicacion.json');
const QA_KB_FILE          = path.join(OWN_DATA_DIR, 'qa_knowledge_base.json');
const REGLAS_NEGOCIO_FILE = path.join(OWN_DATA_DIR, 'reglas_negocio.json');
const REGLAS_NEGOCIO_DEFAULT = path.join(__dirname, 'reglas_negocio.defaults.json');

function similaridad(a, b) {
  const tokenize = s => s.toLowerCase().replace(/[^a-záéíóúüñ0-9\s]/g, '').split(/\s+/).filter(w => w.length > 2);
  const wa = new Set(tokenize(a));
  const wb = new Set(tokenize(b));
  if (!wa.size || !wb.size) return 0;
  let interseccion = 0;
  wa.forEach(w => { if (wb.has(w)) interseccion++; });
  return interseccion / Math.sqrt(wa.size * wb.size);
}

function buscarSimilares(pregunta, n = 8) {
  if (!fs.existsSync(LEARNED_FILE)) return [];
  try {
    const learned = JSON.parse(fs.readFileSync(LEARNED_FILE, 'utf8'));
    return learned
      .map(e => ({ ...e, score: similaridad(pregunta, e.pregunta) }))
      .filter(e => e.score > 0.1)
      .sort((a, b) => b.score - a.score)
      .slice(0, n);
  } catch(e) { return []; }
}

function loadReglasNegocio() {
  // Si el archivo del volumen no existe o está vacío, copiar el base del repo
  if (!fs.existsSync(REGLAS_NEGOCIO_FILE) || fs.statSync(REGLAS_NEGOCIO_FILE).size === 0) {
    if (REGLAS_NEGOCIO_FILE !== REGLAS_NEGOCIO_DEFAULT && fs.existsSync(REGLAS_NEGOCIO_DEFAULT)) {
      try {
        fs.copyFileSync(REGLAS_NEGOCIO_DEFAULT, REGLAS_NEGOCIO_FILE);
        console.log('[reglas] cargadas desde archivo base del repo');
      } catch(e) {}
    }
  }
  if (!fs.existsSync(REGLAS_NEGOCIO_FILE)) return [];
  try {
    const reglas = JSON.parse(fs.readFileSync(REGLAS_NEGOCIO_FILE, 'utf8'));
    if (!reglas.length && REGLAS_NEGOCIO_FILE !== REGLAS_NEGOCIO_DEFAULT && fs.existsSync(REGLAS_NEGOCIO_DEFAULT)) {
      const defaults = JSON.parse(fs.readFileSync(REGLAS_NEGOCIO_DEFAULT, 'utf8'));
      if (defaults.length) {
        fs.writeFileSync(REGLAS_NEGOCIO_FILE, JSON.stringify(defaults, null, 2));
        return defaults;
      }
    }
    return reglas;
  } catch(e) { return []; }
}

function filtrarReglasPorContexto(reglas, contexto) {
  const MAP = {
    'post-venta': ['post-venta', 'envíos', 'retiros', 'general'],
    'preguntas':  ['preguntas', 'envíos', 'general'],
    'tareas':     ['tareas', 'envíos', 'general'],
  };
  const permitidas = MAP[contexto] || null;
  if (!permitidas) return reglas;
  return reglas.filter(r => {
    if (!r.categoria) return true;
    return permitidas.includes(r.categoria.toLowerCase());
  });
}

function reglasTexto(reglas) {
  if (!reglas.length) return '';
  return `\nInformación del negocio:\n${reglas.map(r => `- ${r.categoria ? '[' + r.categoria + '] ' : ''}${r.texto}`).join('\n')}`;
}

// GET /api/config/reglas
app.get('/api/config/reglas', requireToken, (req, res) => {
  res.json(loadReglasNegocio());
});

// POST /api/config/reglas
app.post('/api/config/reglas', requireToken, (req, res) => {
  const { texto, categoria } = req.body;
  if (!texto?.trim()) return res.status(400).json({ error: 'texto requerido' });
  const reglas = loadReglasNegocio();
  const nueva = { id: Date.now(), texto: texto.trim(), categoria: categoria?.trim() || '' };
  reglas.push(nueva);
  fs.writeFileSync(REGLAS_NEGOCIO_FILE, JSON.stringify(reglas, null, 2));
  res.json(nueva);
});

// DELETE /api/config/reglas/:id
app.delete('/api/config/reglas/:id', requireToken, (req, res) => {
  const id = parseInt(req.params.id);
  const reglas = loadReglasNegocio().filter(r => r.id !== id);
  fs.writeFileSync(REGLAS_NEGOCIO_FILE, JSON.stringify(reglas, null, 2));
  res.json({ ok: true });
});

// GET /api/config/reglas/interpretar
app.get('/api/config/reglas/interpretar', requireToken, async (req, res) => {
  if (!anthropic) return res.status(400).json({ error: 'ANTHROPIC_API_KEY no configurada' });
  const reglas = loadReglasNegocio();
  if (!reglas.length) return res.json({ interpretacion: '' });
  try {
    const r = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 400,
      messages: [{
        role: 'user',
        content: `Sos el asistente de MUNDO SHOP. Te dieron las siguientes reglas de negocio para usar al responder clientes en Mercado Libre:

${reglas.map(r => `- ${r.categoria ? '[' + r.categoria + '] ' : ''}${r.texto}`).join('\n')}

Resumí cada regla en formato diagrama de una línea: "situación → acción/dato clave". Una línea por regla, sin explicaciones, sin puntos, directo al grano. Ejemplo: "retiro muebles → Av. Italia 1234"`
      }]
    });
    res.json({ interpretacion: r.content[0].text.trim() });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/ml/buscar — Buscar por orden, tracking o nickname
app.get('/api/ml/buscar', requireToken, async (req, res) => {
  const q = (req.query.q || '').trim();
  if (!q) return res.status(400).json({ error: 'query requerido' });
  const headers = { Authorization: `Bearer ${tokenData.access_token}` };

  try {
    let order = null;
    let shipment = null;
    let claims = [];
    let claimMessages = [];
    let messages = [];

    // 1. Detectar tipo de búsqueda e intentar encontrar la orden
    const isNumeric = /^\d+$/.test(q);

    if (isNumeric && q.length > 10) {
      // Probablemente un order ID o pack ID
      try {
        const r = await axios.get(`${ML_API_URL}/orders/${q}`, { headers });
        order = r.data;
      } catch(e) {
        // Intentar como pack ID
        try {
          const p = await axios.get(`${ML_API_URL}/packs/${q}`, { headers });
          if (p.data.orders?.length) {
            const orderId = p.data.orders[0].id;
            const o = await axios.get(`${ML_API_URL}/orders/${orderId}`, { headers });
            order = o.data;
          }
        } catch {}
      }
    }

    if (!order && isNumeric) {
      // Intentar como shipment ID directo (en Flex el tracking = shipment ID)
      try {
        const s = await axios.get(`${ML_API_URL}/shipments/${q}`, { headers });
        if (s.data?.order_id) {
          shipment = s.data;
          try {
            const o = await axios.get(`${ML_API_URL}/orders/${s.data.order_id}`, { headers });
            order = o.data;
          } catch {}
        }
      } catch {}
    }

    if (!order && !isNumeric) {
      // Buscar por nickname — primero intentar con q parameter
      const nickname = q.toUpperCase();
      try {
        const r = await axios.get(`${ML_API_URL}/orders/search`, {
          params: { seller: tokenData.user_id || 352172083, q: nickname, sort: 'date_desc', limit: 50 },
          headers
        });
        const match = (r.data.results || []).find(o => o.buyer?.nickname === nickname);
        if (match) order = match;
      } catch {}
      // Fallback: recorrer órdenes recientes
      if (!order) {
        let offset = 0;
        while (!order && offset < 500) {
          const r = await axios.get(`${ML_API_URL}/orders/search`, {
            params: { seller: tokenData.user_id || 352172083, sort: 'date_desc', limit: 50, offset },
            headers
          });
          const match = (r.data.results || []).find(o => o.buyer?.nickname === nickname);
          if (match) { order = match; break; }
          offset += 50;
          if ((r.data.results || []).length < 50) break;
        }
      }
    }

    if (!order) return res.json({ found: false, query: q });

    // 2. Obtener shipment si no lo tenemos
    if (!shipment && order.shipping?.id) {
      try {
        const s = await axios.get(`${ML_API_URL}/shipments/${order.shipping.id}`, { headers });
        shipment = s.data;
      } catch {}
    }

    // 3. Buscar claims
    try {
      const c = await axios.get(`${ML_API_URL}/post-purchase/v1/claims/search`, {
        params: { order_id: order.id },
        headers
      });
      claims = c.data.data || [];

      // Obtener mensajes de cada claim
      for (const claim of claims) {
        try {
          const m = await axios.get(`${ML_API_URL}/post-purchase/v1/claims/${claim.id}/messages`, { headers });
          claimMessages.push({ claim_id: claim.id, messages: m.data || [] });
        } catch {}
      }
    } catch {}

    // 3b. Historial del envío
    let shipmentHistory = null;
    if (shipment?.id) {
      try {
        const h = await axios.get(`${ML_API_URL}/shipments/${shipment.id}/history`, { headers });
        shipmentHistory = h.data;
      } catch {}
    }

    // 4. Obtener mensajes post-venta
    const packId = order.pack_id || order.id;
    try {
      const m = await axios.get(`${ML_API_URL}/messages/packs/${packId}/sellers/${order.seller?.id || 352172083}`, {
        headers,
        params: { tag: 'post_sale' }
      });
      messages = m.data.messages || [];
    } catch {}

    // 5. Obtener detalle del item
    let itemDetail = null;
    const firstItem = order.order_items?.[0]?.item;
    if (firstItem?.id) {
      try {
        const ctx = await fetchItemContext(firstItem.id);
        itemDetail = ctx;
      } catch {}
    }

    // 5b. Consultar SoyDelivery si es Flex
    let soydelivery = null;
    let sdHistorial = null;
    if (shipment && shipment.logistic_type === 'self_service') {
      const histData = await consultarSoyDeliveryHistorial(shipment.id);
      if (histData?.PedidoConsultaSDT) {
        const sd = histData.PedidoConsultaSDT;
        sdHistorial = {
          pedido_id: sd.PedidoId,
          estado: sd.PedidoEstado,
          estado_interno: sd.PedidoNegocioEstadoIntNombre,
          fecha_ingreso: sd.PedidoFechaIngreso,
          fecha_entrega: sd.PedidoFechaEntrega,
          fecha_entregado: sd.PedidoFechaEntregado,
          explicacion: sd.PedidoEstadoExplanation,
          historial: (sd.PedidoHistorial || []).map(h => ({
            fecha: h.PedidoHistorialFecha,
            estado: h.PedidoHistorialEstado,
            detalle: h.PedidoHistorialDetalle,
            estado_nombre: h.DiccionarioEstadoNombre,
          })),
        };
        // También consultar estado actual con repartidor
        const sdId = parseInt(sd.PedidoId);
        if (sdId) {
          soydelivery = await consultarSoyDelivery(sdId);
        }
      }
    }

    // 5c. Consultar DAC si es ME1
    let dacData = null;
    if (shipment && shipment.logistic_type === 'default') {
      // Buscar número de guía DAC en mensajes post-venta
      let dacGuia = null;
      for (const m of messages) {
        const match = (m.text || '').match(/seguimiento\s*(?:es)?[:\.\s]*(\d{8,})/i);
        if (match) { dacGuia = match[1]; break; }
      }
      if (dacGuia) {
        const dacResult = await consultarDAC(dacGuia);
        if (dacResult?.data) {
          const d = dacResult.data;
          dacData = {
            guia: dacGuia,
            estado: d.Estado_de_la_Guia,
            destinatario: (d.Destinatario || '').trim(),
            destino: `${(d.Calle_Destinatario || '').trim()}, ${d.Ciudad_Destinatario || ''}, ${d.Estado_Destinatario || ''}`,
            oficina_destino: d.Oficina_Destino,
            oficina_actual: d.D_Oficina_Actual,
            remitente: (d.Remitente || '').trim(),
            persona_recibe: d.Persona_RecibeGuia || '',
            ci_recibe: d.ID_RecibeGuia || '',
            historial: (dacResult.dataHistoria || []).map(h => ({
              estado: h.D_Estado_Guia,
              oficina: h.D_Oficina,
              fecha: h.F_Historia,
              usuario: h.D_Usuario,
            })),
            paquetes: (dacResult.dataPaquete || []).map(p => `${p.Cantidad} x ${p.D_Tipo_Empaque}`),
          };
        }
      }
    }

    // 6. Armar respuesta
    res.json({
      found: true,
      dac: dacData,
      order: {
        id: order.id,
        status: order.status,
        date_created: order.date_created,
        total_amount: order.total_amount,
        currency_id: order.currency_id,
        buyer: {
          id: order.buyer?.id,
          nickname: order.buyer?.nickname,
          first_name: order.buyer?.first_name,
          last_name: order.buyer?.last_name,
        },
        items: order.order_items?.map(i => ({
          id: i.item?.id,
          title: i.item?.title,
          quantity: i.quantity,
          unit_price: i.unit_price,
          sku: i.item?.seller_sku,
        })),
      },
      shipping: shipment ? {
        id: shipment.id,
        status: shipment.status,
        substatus: shipment.substatus,
        logistic_type: shipment.logistic_type || '',
        tracking_number: shipment.tracking_number,
        tracking_url: shipment.tracking_url,
        date_created: shipment.date_created,
        last_updated: shipment.last_updated,
        receiver_address: shipment.receiver_address ? {
          city: shipment.receiver_address.city?.name,
          state: shipment.receiver_address.state?.name,
          street: shipment.receiver_address.street_name,
          number: shipment.receiver_address.street_number,
          zip_code: shipment.receiver_address.zip_code,
        } : null,
        tracking_method: shipmentHistory?.tracking_method || shipment.tracking_method || '',
        history: shipment.status_history || shipmentHistory?.date_history || null,
      } : null,
      claims: claims.map(c => {
        const sellerActions = (c.players || []).find(p => p.role === 'respondent')?.available_actions || [];
        return {
          id: c.id,
          status: c.status,
          type: c.type,
          stage: c.stage,
          reason_id: c.reason_id,
          date_created: c.date_created,
          last_updated: c.last_updated,
          resolution: c.resolution,
          available_actions: sellerActions.map(a => ({ action: a.action, due_date: a.due_date })),
          messages: (claimMessages.find(cm => cm.claim_id === c.id)?.messages || []).map(m => ({
            from: m.sender_role,
            text: m.message,
            date: m.date_created,
          })),
        };
      }),
      messages: messages.map(m => ({
        from: m.from?.user_id === order.buyer?.id ? 'comprador' : 'vendedor',
        text: m.text,
        date: m.date_created,
      })),
      item_detail: itemDetail,
      soydelivery: soydelivery ? {
        estado: soydelivery.Pedido_estado_desc,
        estado_id: soydelivery.Pedido_estado_id,
        delivery_nombre: soydelivery.Delivery_nombre_apellido,
        delivery_telefono: soydelivery.Delivery_telefono,
        delivery_ubicacion: soydelivery.Delivery_location,
        fecha_entrega: soydelivery.Fecha_entrega,
        franja_horaria: soydelivery.Franja_horaria_desc,
        fecha_estimada: soydelivery.Fecha_estimada_entrega,
      } : null,
      sd_historial: sdHistorial,
    });
  } catch(e) {
    console.error('[buscar]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── Retiros cache ──
let retirosCache = null;
let retirosUpdating = false;
let retirosProgress = { current: 0, total: 0 };
const RETIROS_CACHE_FILE = path.join(OWN_DATA_DIR, 'retiros_cache.json');

// Cargar cache de disco al arrancar
try { if (fs.existsSync(RETIROS_CACHE_FILE)) { retirosCache = JSON.parse(fs.readFileSync(RETIROS_CACHE_FILE, 'utf8')); console.log(`[retiros] cache cargado: ${retirosCache.total} envíos activos`); } } catch {}

async function actualizarRetiros() {
  if (retirosUpdating || !tokenData?.access_token) return;
  retirosUpdating = true;
  console.log('[retiros] actualizando...');
  try {
    const headers = { Authorization: `Bearer ${tokenData.access_token}` };
    const sellerId = tokenData.user_id || 352172083;

    let allOrders = [];
    let offset = 0;
    while (offset < 200) {
      const r = await axios.get(`${ML_API_URL}/orders/search`, {
        params: { seller: sellerId, sort: 'date_desc', limit: 50, offset, 'order.status': 'paid' },
        headers
      });
      const batch = r.data.results || [];
      allOrders = allOrders.concat(batch);
      offset += 50;
      if (batch.length < 50) break;
    }

    const soydelivery = [];
    const robert = [];
    const dac = [];
    const mercadoenvios = [];

    retirosProgress = { current: 0, total: allOrders.length };
    for (const o of allOrders) {
      retirosProgress.current++;
      if (!o.shipping?.id) continue;
      let ship;
      try {
        const s = await axios.get(`${ML_API_URL}/shipments/${o.shipping.id}`, { headers });
        ship = s.data;
      } catch { continue; }

      if (ship.status === 'cancelled') continue;

      const item = o.order_items?.[0]?.item;
      const entry = {
        order_id: o.id,
        pack_id: o.pack_id,
        buyer: o.buyer?.nickname,
        buyer_name: `${o.buyer?.first_name || ''} ${o.buyer?.last_name || ''}`.trim(),
        item_title: item?.title || '',
        item_id: item?.id,
        quantity: o.order_items?.[0]?.quantity || 1,
        ship_status: ship.status,
        ship_substatus: ship.substatus,
        logistic_type: ship.logistic_type,
        tracking: ship.tracking_number,
        date_created: o.date_created,
        receiver_city: ship.receiver_address?.city?.name || '',
        receiver_state: ship.receiver_address?.state?.name || '',
        receiver_phone: ship.receiver_address?.receiver_phone || null,
        receiver_name: ship.receiver_address?.receiver_name || '',
      };

      // Determinar cadetería y estado del pipeline
      if (ship.logistic_type === 'self_service') {
        const sdHist = await consultarSoyDeliveryHistorial(ship.id);
        if (sdHist?.PedidoConsultaSDT) {
          const sd = sdHist.PedidoConsultaSDT;
          entry.sd_estado = sd.PedidoNegocioEstadoIntNombre;
          entry.sd_estado_id = sd.PedidoEstado;
          const retiroEvento = (sd.PedidoHistorial || []).find(h => h.PedidoHistorialEstado === 'R');
          entry.fecha_retiro = retiroEvento?.PedidoHistorialFecha || null;
          entry.retirado_por = retiroEvento ? retiroEvento.PedidoHistorialDetalle.replace(/^Pedido retirado por\s*/i, '') : null;
          entry.cadeteria = 'SoyDelivery';

          // Pipeline: pendiente → en_camino_sin_escaneo → en_camino → entregado
          if (ship.status === 'delivered' || sd.PedidoEstado === 'E') {
            entry.etapa = 'entregado';
          } else if (retiroEvento) {
            entry.etapa = 'en_camino';
          } else if (ship.status === 'shipped' && !retiroEvento) {
            entry.etapa = 'en_camino_sin_escaneo';
          } else {
            entry.etapa = 'pendiente';
          }
          soydelivery.push(entry);
        } else {
          entry.cadeteria = 'Robert';
          if (ship.status === 'delivered') entry.etapa = 'entregado';
          else if (ship.status === 'shipped') entry.etapa = 'en_camino';
          else entry.etapa = 'pendiente';
          robert.push(entry);
        }
      } else if (ship.logistic_type === 'default') {
        entry.cadeteria = 'DAC';
        // Buscar guía DAC y mensajes del comprador
        const packId = o.pack_id || o.id;
        let allMsgs = [];
        try {
          const m = await axios.get(`${ML_API_URL}/messages/packs/${packId}/sellers/${sellerId}`, { headers, params: { tag: 'post_sale' } });
          allMsgs = m.data.messages || [];
          for (const msg of allMsgs) {
            const match = (msg.text || '').match(/seguimiento\s*(?:es)?[:\.\s]*(\d{8,})/i);
            if (match) { entry.dac_guia = match[1]; break; }
          }
        } catch {}
        // Detectar mensajes no leídos del comprador pidiendo envío
        const buyerMsgs = allMsgs.filter(msg => msg.from?.user_id === o.buyer?.id);
        const unreadBuyer = buyerMsgs.filter(msg => !msg.date_read);
        const pideEnvio = buyerMsgs.some(msg => (msg.text||'').toLowerCase().match(/env[ií]o|enviar|mandar|domicilio|llegar|lleg[uo]|direcci[oó]n|mand[ae]|despacho/));
        entry.tiene_mensajes = buyerMsgs.length > 0;
        entry.mensajes_no_leidos = unreadBuyer.length;
        entry.pide_envio = pideEnvio;
        entry.ultimo_msg_buyer = buyerMsgs.length ? (buyerMsgs[0].text || '').slice(0, 100) : null;
        // Consultar estado en DAC si tenemos guía
        if (entry.dac_guia) {
          const dacResult = await consultarDAC(entry.dac_guia);
          if (dacResult?.data) {
            entry.dac_estado = dacResult.data.Estado_de_la_Guia;
            entry.dac_destino = dacResult.data.Oficina_Destino;

            // Auto-upload: si DAC ya retiró pero ML no tiene tracking, subirlo
            const dacRetiro = entry.dac_estado && entry.dac_estado !== 'REGISTRADA' && entry.dac_estado !== 'DOCUMENTADA';
            if (dacRetiro && ship.status === 'pending' && !ship.tracking_number) {
              try {
                await axios.put(`${ML_API_URL}/shipments/${o.shipping.id}`, {
                  tracking_number: entry.dac_guia,
                  tracking_method: 'DAC',
                  service_id: 282604
                }, { headers: { ...headers, 'Content-Type': 'application/json' } });
                console.log(`[dac-sync] Shipment ${o.shipping.id} → shipped con guía ${entry.dac_guia}`);
                entry.ship_status = 'shipped';
              } catch(e) {
                console.error(`[dac-sync] Error subiendo tracking ${o.shipping.id}:`, e.response?.data?.message || e.message);
              }
            }

            // Auto-deliver: si DAC dice ENTREGADA y ML no está delivered
            if (entry.dac_estado === 'ENTREGADA' && ship.status !== 'delivered') {
              try {
                // Si todavía no tiene tracking, primero subirlo
                if (!ship.tracking_number) {
                  await axios.put(`${ML_API_URL}/shipments/${o.shipping.id}`, {
                    tracking_number: entry.dac_guia,
                    tracking_method: 'DAC',
                    service_id: 282604
                  }, { headers: { ...headers, 'Content-Type': 'application/json' } });
                }
                await axios.put(`${ML_API_URL}/shipments/${o.shipping.id}`, {
                  status: 'delivered',
                  service_id: 282604
                }, { headers: { ...headers, 'Content-Type': 'application/json' } });
                console.log(`[dac-sync] Shipment ${o.shipping.id} → delivered`);
                entry.ship_status = 'delivered';
              } catch(e) {
                console.error(`[dac-sync] Error marcando delivered ${o.shipping.id}:`, e.response?.data?.message || e.message);
              }
            }

            if (entry.dac_estado === 'ENTREGADA') entry.etapa = 'entregado';
            else if (dacRetiro) entry.etapa = 'en_camino';
            else entry.etapa = 'pendiente';
          } else {
            entry.etapa = 'pendiente';
          }
        } else {
          if (ship.status === 'delivered') entry.etapa = 'entregado';
          else if (ship.status === 'shipped') entry.etapa = 'en_camino';
          else entry.etapa = 'pendiente';
        }
        dac.push(entry);
      } else {
        entry.cadeteria = 'Mercado Envíos';
        if (ship.status === 'delivered') entry.etapa = 'entregado';
        else if (ship.status === 'shipped') entry.etapa = 'en_camino';
        else entry.etapa = 'pendiente';
        mercadoenvios.push(entry);
      }
    }

    const allEntries = [...soydelivery, ...robert, ...dac, ...mercadoenvios];

    retirosCache = {
      total: allEntries.length,
      pendientes: allEntries.filter(e => e.etapa === 'pendiente'),
      en_camino_sin_escaneo: allEntries.filter(e => e.etapa === 'en_camino_sin_escaneo'),
      en_camino: allEntries.filter(e => e.etapa === 'en_camino'),
      entregados: allEntries.filter(e => e.etapa === 'entregado'),
      soydelivery, robert, dac, mercadoenvios,
      updated_at: new Date().toISOString(),
    };
    try { fs.writeFileSync(RETIROS_CACHE_FILE, JSON.stringify(retirosCache)); } catch {}
    console.log(`[retiros] actualizado: ${retirosCache.total} envíos activos`);
  } catch(e) {
    console.error('[retiros] error:', e.message);
  } finally {
    retirosUpdating = false;
  }
}

// Actualizar al arrancar (después de 10s para que el token esté listo) y cada 5 min
setTimeout(() => actualizarRetiros(), 10000);
setInterval(() => actualizarRetiros(), 2 * 60 * 1000);

// GET /api/ml/retiros — Devuelve cache, opcionalmente fuerza refresh
app.get('/api/ml/retiros', requireToken, async (req, res) => {
  if (req.query.refresh === '1') {
    await actualizarRetiros();
  }
  if (retirosCache) {
    res.json({ ...retirosCache, loading: retirosUpdating, progress: retirosProgress });
  } else {
    res.json({ total: 0, pendientes: [], retirados_hoy: [], soydelivery: [], robert: [], dac: [], mercadoenvios: [], updated_at: null, loading: retirosUpdating, progress: retirosProgress });
  }
});

// ── Envíos por cadete ──

// GET /api/retira-local — Pedidos que retiran en el local
app.get('/api/retira-local', requireToken, async (req, res) => {
  try {
    const headers = { Authorization: `Bearer ${tokenData.access_token}` };
    const sellerId = tokenData.user_id || 352172083;
    let items = [];
    let offset = 0;
    while (offset < 200) {
      const r = await axios.get(`${ML_API_URL}/orders/search`, {
        params: { seller: sellerId, sort: 'date_desc', limit: 50, offset, 'order.status': 'paid' },
        headers
      });
      for (const o of (r.data.results || [])) {
        if (o.shipping?.id) continue; // tiene envío, no es retiro en local
        const item = o.order_items?.[0]?.item;
        // Buscar mensajes
        let msgs = [];
        const packId = o.pack_id || o.id;
        try {
          const m = await axios.get(`${ML_API_URL}/messages/packs/${packId}/sellers/${sellerId}`, { headers, params: { tag: 'post_sale' } });
          msgs = m.data.messages || [];
        } catch {}
        const buyerMsgs = msgs.filter(m => m.from?.user_id === o.buyer?.id);
        const unread = buyerMsgs.filter(m => !m.date_read);

        // Filtrar: si el COMPRADOR pide envío en sus mensajes, no es retiro en local
        const pideEnvio = buyerMsgs.some(m => (m.text||'').toLowerCase().match(/env[ií]o|enviar|mandar|domicilio|despacho|manden|mand[ae]/));
        if (pideEnvio) continue;

        items.push({
          order_id: o.id,
          pack_id: o.pack_id,
          buyer: o.buyer?.nickname,
          buyer_name: `${o.buyer?.first_name || ''} ${o.buyer?.last_name || ''}`.trim(),
          item_title: item?.title || '',
          item_id: item?.id,
          quantity: o.order_items?.[0]?.quantity || 1,
          total_amount: o.total_amount,
          date_created: o.date_created,
          mensajes_no_leidos: unread.length,
          ultimo_msg: buyerMsgs.length ? (buyerMsgs[0].text || '').slice(0, 100) : null,
        });
      }
      offset += 50;
      if ((r.data.results || []).length < 50) break;
    }
    res.json({ items, total: items.length });
  } catch(e) {
    console.error('[retira-local]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/envios/dac — Lista envíos DAC + buscar por guía
app.get('/api/envios/dac', requireToken, async (req, res) => {
  const q = (req.query.q || '').trim();
  const items = retirosCache?.dac || [];

  if (q) {
    // Buscar directamente en DAC por número de guía
    const dacResult = await consultarDAC(q);
    if (dacResult?.data) {
      const d = dacResult.data;
      return res.json({
        found: true,
        guia: q,
        estado: d.Estado_de_la_Guia,
        destinatario: (d.Destinatario || '').trim(),
        remitente: (d.Remitente || '').trim(),
        destino: `${(d.Calle_Destinatario || '').trim()} ${d.Nro_Puerta_Destinatario || ''}, ${d.Ciudad_Destinatario || ''}, ${d.Estado_Destinatario || ''}`,
        oficina_destino: d.Oficina_Destino,
        oficina_actual: d.D_Oficina_Actual,
        persona_recibe: d.Persona_RecibeGuia || '',
        ci_recibe: d.ID_RecibeGuia || '',
        paquetes: (dacResult.dataPaquete || []).map(p => `${p.Cantidad} x ${p.D_Tipo_Empaque}`),
        historial: (dacResult.dataHistoria || []).map(h => ({
          estado: h.D_Estado_Guia,
          oficina: h.D_Oficina,
          fecha: h.F_Historia,
          usuario: h.D_Usuario,
        })),
      });
    }
    // Buscar en cache por guía, order_id o buyer
    const filtered = items.filter(e =>
      (e.dac_guia && e.dac_guia.includes(q)) ||
      String(e.order_id).includes(q) ||
      (e.buyer || '').toLowerCase().includes(q.toLowerCase()) ||
      (e.buyer_name || '').toLowerCase().includes(q.toLowerCase())
    );
    return res.json({ found: filtered.length > 0, items: filtered, total: filtered.length });
  }

  const acuerda = items.filter(e => !e.dac_guia && e.pide_envio && e.mensajes_no_leidos > 0);
  const sin_coordinar = items.filter(e => !e.dac_guia && !(e.pide_envio && e.mensajes_no_leidos > 0));
  const coordinados = items.filter(e => e.dac_guia && (!e.dac_estado || e.dac_estado === 'REGISTRADA' || e.dac_estado === 'DOCUMENTADA'));
  const retirados = items.filter(e => e.dac_guia && e.dac_estado && e.dac_estado !== 'REGISTRADA' && e.dac_estado !== 'DOCUMENTADA' && e.dac_estado !== 'ENTREGADA');
  const entregados = items.filter(e => e.dac_estado === 'ENTREGADA');
  res.json({ items, total: items.length, acuerda, sin_coordinar, coordinados, retirados, entregados, updated_at: retirosCache?.updated_at });
});

// GET /api/envios/soydelivery — Lista envíos SoyDelivery + buscar por ID
app.get('/api/envios/soydelivery', requireToken, async (req, res) => {
  const q = (req.query.q || '').trim();
  const items = retirosCache?.soydelivery || [];

  if (q) {
    const histData = await consultarSoyDeliveryHistorial(q);
    if (histData?.PedidoConsultaSDT) {
      const sd = histData.PedidoConsultaSDT;
      const sdId = parseInt(sd.PedidoId);
      let detalle = null;
      if (sdId) detalle = await consultarSoyDelivery(sdId);
      return res.json({
        found: true,
        pedido_id: sd.PedidoId,
        estado: sd.PedidoEstado,
        estado_nombre: sd.PedidoNegocioEstadoIntNombre,
        fecha_ingreso: sd.PedidoFechaIngreso,
        fecha_entrega: sd.PedidoFechaEntrega,
        fecha_entregado: sd.PedidoFechaEntregado,
        explicacion: sd.PedidoEstadoExplanation,
        delivery: detalle ? {
          nombre: detalle.Delivery_nombre_apellido,
          telefono: detalle.Delivery_telefono,
          ubicacion: detalle.Delivery_location,
          franja: detalle.Franja_horaria_desc,
          fecha_estimada: detalle.Fecha_estimada_entrega,
        } : null,
        historial: (sd.PedidoHistorial || []).map(h => ({
          fecha: h.PedidoHistorialFecha,
          estado: h.PedidoHistorialEstado,
          detalle: h.PedidoHistorialDetalle,
          estado_nombre: h.DiccionarioEstadoNombre,
        })),
      });
    }
    const filtered = items.filter(e =>
      String(e.order_id).includes(q) ||
      String(e.tracking).includes(q) ||
      (e.buyer || '').toLowerCase().includes(q.toLowerCase()) ||
      (e.buyer_name || '').toLowerCase().includes(q.toLowerCase())
    );
    return res.json({ found: filtered.length > 0, items: filtered, total: filtered.length });
  }

  const pendientes = items.filter(e => e.etapa === 'pendiente');
  const en_camino_sin_escaneo = items.filter(e => e.etapa === 'en_camino_sin_escaneo');
  const en_camino = items.filter(e => e.etapa === 'en_camino');
  const entregados = items.filter(e => e.etapa === 'entregado');
  res.json({ items, total: items.length, pendientes, en_camino_sin_escaneo, en_camino, entregados, updated_at: retirosCache?.updated_at });
});

// GET /api/envios/deri — Lista envíos Robert/Deri + buscar por ID
app.get('/api/envios/deri', requireToken, async (req, res) => {
  const q = (req.query.q || '').trim();
  const items = retirosCache?.robert || [];

  if (q) {
    const deriOrder = await consultarDeriOrder(q);
    if (deriOrder) {
      const statuses = await consultarDeriStatuses(q) || [];
      return res.json({ found: true, order: deriOrder, historial: statuses });
    }
    const filtered = items.filter(e =>
      String(e.order_id).includes(q) ||
      String(e.tracking).includes(q) ||
      (e.buyer || '').toLowerCase().includes(q.toLowerCase()) ||
      (e.buyer_name || '').toLowerCase().includes(q.toLowerCase())
    );
    return res.json({ found: filtered.length > 0, items: filtered, total: filtered.length });
  }

  const pendientes = items.filter(e => e.etapa === 'pendiente');
  const en_camino = items.filter(e => e.etapa === 'en_camino');
  const entregados = items.filter(e => e.etapa === 'entregado');
  res.json({ items, total: items.length, pendientes, en_camino, entregados, updated_at: retirosCache?.updated_at });
});

// ── Deri (Robert) Webhook ──
// ── DAC Webhook ──
app.post('/webhook/dac', (req, res) => {
  console.log('[dac/webhook]:', JSON.stringify(req.body).slice(0, 500));
  res.json({ ok: true });
});

app.post('/webhook/deri', (req, res) => {
  console.log('[deri/webhook]:', JSON.stringify(req.body).slice(0, 500));
  res.json({ ok: true });
});

// ── SoyDelivery Webhooks ──
// Recibe notificaciones de SoyDelivery y guarda el mapeo shipment_id → sd_pedido_id
app.post('/webhook/soydelivery/:event', (req, res) => {
  const event = req.params.event;
  const data = req.body;
  console.log(`[soydelivery/webhook] ${event}:`, JSON.stringify(data).slice(0, 300));

  const pedidoId = data.Pedido_id || data.pedido_id;
  const externalId = data.Pedido_external_id || data.pedido_external_id;

  if (pedidoId && externalId) {
    sdMapping[externalId] = pedidoId;
    saveSdMapping();
    console.log(`[soydelivery] mapping: ${externalId} → ${pedidoId}`);
  }

  res.json({ ok: true });
});

// Endpoint genérico para cualquier webhook de SoyDelivery
app.post('/webhook/soydelivery', (req, res) => {
  const data = req.body;
  console.log('[soydelivery/webhook]:', JSON.stringify(data).slice(0, 300));

  const pedidoId = data.Pedido_id || data.pedido_id;
  const externalId = data.Pedido_external_id || data.pedido_external_id;

  if (pedidoId && externalId) {
    sdMapping[externalId] = pedidoId;
    saveSdMapping();
  }

  res.json({ ok: true });
});

// GET /api/ml/preguntas/pendientes
app.get('/api/ml/preguntas/pendientes', requireToken, async (req, res) => {
  try {
    const dias = parseInt(req.query.dias) || 7;
    const status = req.query.status === 'ANSWERED' ? 'ANSWERED' : 'UNANSWERED';
    const cutoff = new Date(Date.now() - dias * 24 * 60 * 60 * 1000);
    let questions = [];
    let offset = 0;
    let totalMl = 0;
    const PAGE = 50;
    let keepGoing = true;
    while (keepGoing) {
      const r = await axios.get(`${ML_API_URL}/my/received_questions/search`, {
        params: { status, limit: PAGE, offset, sort_fields: 'date_created', sort_types: 'DESC' },
        headers: { Authorization: `Bearer ${tokenData.access_token}` }
      });
      totalMl = r.data.total || 0;
      const batch = r.data.questions || [];
      if (!batch.length) break;
      for (const q of batch) {
        if (dias > 0 && new Date(q.date_created) < cutoff) { keepGoing = false; break; }
        questions.push(q);
      }
      offset += PAGE;
      if (offset >= totalMl) break;
    }
    const filtered = questions;

    const itemMap = {};
    cachedItems.forEach(i => { itemMap[i.id] = i; });
    const enriched = await Promise.all(filtered.map(async q => {
      const item = itemMap[q.item_id] || {};
      let item_title = item.title || '';
      let item_thumbnail = item.thumbnail || '';
      let item_permalink = item.permalink || '';
      if (q.item_id && (!item_title || !item_thumbnail || !item_permalink)) {
        const ctx = await fetchItemContext(q.item_id).catch(() => null);
        if (ctx) {
          if (!item_title) item_title = ctx.title || q.item_id;
          if (!item_thumbnail) item_thumbnail = ctx.thumbnail || '';
          if (!item_permalink) item_permalink = ctx.permalink || '';
        }
      }
      return {
        id: q.id,
        item_id: q.item_id,
        item_title: item_title || q.item_id,
        item_thumbnail,
        item_permalink,
        text: q.text,
        date_created: q.date_created,
        from_id: q.from?.id,
        status: q.status,
        answer: q.answer ? { text: q.answer.text, date_created: q.answer.date_created } : null
      };
    }));
    res.json({ questions: enriched, total: enriched.length, total_ml: totalMl });
  } catch(e) {
    console.error('[preguntas/pendientes]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// Helper: trae atributos + descripción de un ítem (cache en DATA_DIR compartido)
const ITEM_CACHE_FILE = path.join(DATA_DIR, 'item_detail_cache.json');
const itemDetailCache = (() => {
  try { return fs.existsSync(ITEM_CACHE_FILE) ? JSON.parse(fs.readFileSync(ITEM_CACHE_FILE, 'utf8')) : {}; } catch { return {}; }
})();
function saveItemCache() {
  try { fs.writeFileSync(ITEM_CACHE_FILE, JSON.stringify(itemDetailCache)); } catch {}
}

async function fetchItemContext(itemId) {
  if (itemDetailCache[itemId]) return itemDetailCache[itemId];
  try {
    const [itemR, descR] = await Promise.allSettled([
      axios.get(`${ML_API_URL}/items/${itemId}`, { headers: { Authorization: `Bearer ${tokenData.access_token}` } }),
      axios.get(`${ML_API_URL}/items/${itemId}/description`, { headers: { Authorization: `Bearer ${tokenData.access_token}` } })
    ]);
    const item = itemR.status === 'fulfilled' ? itemR.value.data : {};
    const desc = descR.status === 'fulfilled' ? descR.value.data?.plain_text || '' : '';
    const attrs = (item.attributes || [])
      .filter(a => a.value_name)
      .map(a => `${a.name}: ${a.value_name}`)
      .join(', ');
    const shipping = item.shipping || {};
    const shippingInfo = {
      free_shipping: !!shipping.free_shipping,
      logistic_type: shipping.logistic_type || '',
      local_pick_up: !!shipping.local_pick_up,
      store_pick_up: !!shipping.store_pick_up
    };
    const variations = (item.variations || [])
      .filter(v => v.available_quantity > 0 && v.attribute_combinations?.length)
      .map(v => v.attribute_combinations.map(a => a.value_name).join(' / '));
    const ctx = {
      title: item.title || itemId,
      price: item.price,
      thumbnail: item.thumbnail || '',
      permalink: item.permalink || '',
      shipping: shippingInfo,
      variations,
      attrs,
      description: desc.slice(0, 800)
    };
    itemDetailCache[itemId] = ctx;
    saveItemCache();
    return ctx;
  } catch(e) {
    return { title: itemId, attrs: '', description: '' };
  }
}

function buildItemContextText(ctx) {
  let text = `Producto: ${ctx.title}`;
  if (ctx.price) text += `\nPrecio: $${ctx.price}`;
  if (ctx.shipping) {
    if (ctx.shipping.free_shipping) {
      text += `\nEnvío: GRATIS a todo el país`;
    } else {
      text += `\nEnvío: con costo (calculado por Mercado Libre según destino)`;
    }
    if (ctx.shipping.local_pick_up) text += `\nRetiro en persona: disponible`;
  }
  if (ctx.variations?.length) text += `\nVariantes disponibles: ${ctx.variations.join(', ')}`;
  if (ctx.attrs) text += `\nAtributos: ${ctx.attrs}`;
  if (ctx.description) text += `\nDescripción: ${ctx.description}`;
  return text;
}

// POST /api/ml/preguntas/simular
app.post('/api/ml/preguntas/simular', requireToken, async (req, res) => {
  if (!anthropic) return res.status(400).json({ error: 'ANTHROPIC_API_KEY no configurada' });
  const { questions } = req.body;
  if (!questions?.length) return res.status(400).json({ error: 'questions requerido' });

  let kb = null;
  if (fs.existsSync(QA_KB_FILE)) kb = JSON.parse(fs.readFileSync(QA_KB_FILE, 'utf8'));

  const kbText = kb ? `Estilo MUNDO SHOP:
- Saludo: "${kb.estilo.saludo}"
- Despedida: "${kb.estilo.despedida}"
- Tono: ${kb.estilo.tono}
Reglas clave:
${kb.reglas_generales.slice(0, 10).map(r => '- ' + r).join('\n')}` : '';

  const reglasText = reglasTexto(filtrarReglasPorContexto(loadReglasNegocio(), 'preguntas'));

  let preguntasData = null;
  if (fs.existsSync(PREGUNTAS_FILE)) {
    try { preguntasData = JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8')); } catch(e) {}
  }

  const results = [];
  for (const q of questions) {
    try {
      const itemCtx = await fetchItemContext(q.item_id);
      const itemText = buildItemContextText(itemCtx);

      let ejemplos = '';
      if (preguntasData && q.item_id && preguntasData.byPub[q.item_id]) {
        const prevQA = preguntasData.byPub[q.item_id].qa.slice(-8);
        if (prevQA.length) {
          ejemplos = '\nEjemplos anteriores de esta publicación:\n' +
            prevQA.map(e => `P: ${e.q}\nR: ${e.a}`).join('\n---\n');
        }
      }
      const similares = buscarSimilares(q.text, 6);
      if (similares.length) {
        ejemplos += '\nRespuestas validadas similares:\n' +
          similares.map(e => `P: ${e.pregunta}\nR: ${e.respuesta}`).join('\n---\n');
      }
      // Detectar si la pregunta es un cierre o agradecimiento
      const qTextLower = q.text?.trim().toLowerCase() || '';
      const cierresPre = ['gracias', 'ok', 'dale', 'listo', 'perfecto', 'buenísimo', 'buenisimo', 'entendido', 'de acuerdo'];
      const esCierrePre = cierresPre.some(c => qTextLower === c || qTextLower === c + '.' || qTextLower === c + '!');

      if (esCierrePre) {
        results.push({ id: q.id, respuesta: '¡Con gusto! Quedamos a las órdenes 😊 MUNDO SHOP' });
        continue;
      }

      const r = await anthropic.messages.create({
        model: 'claude-sonnet-4-6',
        max_tokens: 250,
        messages: [{
          role: 'user',
          content: `Sos el equipo de atención al cliente de MUNDO SHOP en Mercado Libre Uruguay.
Soná como una persona real: cercana, amigable y profesional. Usá lenguaje rioplatense natural (vos, te, etc).
${kbText}
${reglasText ? 'REGLAS DEL NEGOCIO (tienen prioridad, usá estos datos exactos):' + reglasText : ''}
${ejemplos}
${itemText}
Pregunta: "${q.text}"

Instrucciones:
- Respondé SOLO con el texto final, sin explicaciones ni comillas
- Saludá con "¡Hola!" y respondé directo — sin frases de relleno como "Buena pregunta", "Claro que sí", "Por supuesto"
- Usá emojis SOLO si el contexto es informal o positivo. En preguntas técnicas, de medidas o reclamos NO uses emojis
- Cerrá con "¡Cualquier otra consulta nos avisás! MUNDO SHOP"
- MUNDO SHOP aparece UNA SOLA VEZ, al cerrar
- NUNCA uses "Agradecemos te hayas comunicado" ni frases corporativas
- Mencioná la dirección de retiro SOLO si preguntan cómo retirar algo ya comprado
- Respondé ÚNICAMENTE lo que preguntó el comprador, sin agregar info extra que no pidió
- Para referirte al producto usá el tipo genérico ("este sillón", "esta mesa", "este mueble"), NUNCA el nombre comercial completo
- Sé breve y directo, máximo 2-3 oraciones
- Si no tenés el dato exacto que pide el comprador, usá tu conocimiento general para dar una medida o referencia estándar del rubro, aclarando que es aproximada. NUNCA derives al cliente a otro lado ni prometas gestiones internas
- No inventes datos específicos del producto, pero sí podés dar referencias estándar cuando aplica`
        }]
      });
      results.push({ id: q.id, respuesta: r.content[0].text.trim() });
    } catch(e) {
      results.push({ id: q.id, error: e.message });
    }
  }
  res.json({ results });
});

const LEARNED_FILE  = path.join(OWN_DATA_DIR, 'respuestas_aprendidas.json');
const BAD_RESP_FILE = path.join(OWN_DATA_DIR, 'respuestas_malas.json');

// POST /api/ml/mensajes/feedback-malo
app.post('/api/ml/mensajes/feedback-malo', requireToken, async (req, res) => {
  const { historial, respuesta_mala, motivo } = req.body;
  if (!respuesta_mala) return res.status(400).json({ error: 'respuesta_mala requerida' });
  try {
    let malas = fs.existsSync(BAD_RESP_FILE) ? JSON.parse(fs.readFileSync(BAD_RESP_FILE, 'utf8')) : [];
    malas.push({ historial: historial || '', respuesta_mala, motivo: motivo || '', fecha: new Date().toISOString() });
    if (malas.length > 500) malas = malas.slice(-500);
    fs.writeFileSync(BAD_RESP_FILE, JSON.stringify(malas, null, 2));

    // Generar regla automática a partir del feedback si hay motivo y anthropic disponible
    if (motivo && anthropic) {
      (async () => {
        try {
          const r = await anthropic.messages.create({
            model: 'claude-haiku-4-5',
            max_tokens: 150,
            messages: [{
              role: 'user',
              content: `Sos un asistente que genera reglas de comportamiento para un chatbot de atención al cliente.
Se marcó esta respuesta como mala:
"${respuesta_mala.slice(0, 300)}"

Motivo: "${motivo}"

Generá UNA regla corta y concreta (máx 20 palabras) para que el chatbot no cometa este error en el futuro.
Empezá con un verbo en infinitivo (ej: "No mencionar...", "Evitar...", "Responder solo...").
Respondé ÚNICAMENTE con el texto de la regla, sin explicaciones.`
            }]
          });
          const reglaTexto = r.content[0].text.trim();
          if (reglaTexto) {
            const reglas = loadReglasNegocio();
            // Evitar duplicados similares
            const yaExiste = reglas.some(reg => reg.texto.toLowerCase().includes(reglaTexto.slice(0, 30).toLowerCase()));
            if (!yaExiste) {
              reglas.push({ id: Date.now(), categoria: 'respuestas', texto: reglaTexto, auto: true });
              fs.writeFileSync(REGLAS_NEGOCIO_FILE, JSON.stringify(reglas, null, 2));
              console.log(`[feedback] regla auto-generada: "${reglaTexto}"`);
            }
          }
        } catch(e) {
          console.error('[feedback] error generando regla:', e.message);
        }
      })();
    }

    res.json({ ok: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/ml/preguntas/feedback
app.post('/api/ml/preguntas/feedback', requireToken, (req, res) => {
  const { pregunta, respuesta, item_id, item_title, tipo } = req.body;
  if (!pregunta || !respuesta) return res.status(400).json({ error: 'pregunta y respuesta requeridos' });
  try {
    let learned = [];
    if (fs.existsSync(LEARNED_FILE)) learned = JSON.parse(fs.readFileSync(LEARNED_FILE, 'utf8'));
    learned.push({ pregunta, respuesta, item_id, item_title, tipo: tipo || 'pregunta', fecha: new Date().toISOString() });
    if (learned.length > 500) learned = learned.slice(-500);
    fs.writeFileSync(LEARNED_FILE, JSON.stringify(learned, null, 2));
    if (item_id && fs.existsSync(PREGUNTAS_FILE)) {
      const data = JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8'));
      if (!data.byPub[item_id]) data.byPub[item_id] = { titulo: item_title || item_id, qa: [] };
      data.byPub[item_id].qa.push({ q: pregunta, a: respuesta, aprendida: true });
      fs.writeFileSync(PREGUNTAS_FILE, JSON.stringify(data));
    }
    res.json({ ok: true, total_aprendidas: learned.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/ml/preguntas/responder-ml
app.post('/api/ml/preguntas/responder-ml', requireToken, async (req, res) => {
  const { question_id, text, pregunta, item_id, item_title } = req.body;
  if (!question_id || !text) return res.status(400).json({ error: 'question_id y text requeridos' });
  try {
    const r = await axios.post(`${ML_API_URL}/answers`,
      { question_id, text },
      { headers: { Authorization: `Bearer ${tokenData.access_token}`, 'Content-Type': 'application/json' } }
    );
    if (pregunta) {
      try {
        let learned = [];
        if (fs.existsSync(LEARNED_FILE)) learned = JSON.parse(fs.readFileSync(LEARNED_FILE, 'utf8'));
        const yaExiste = learned.some(e => e.pregunta === pregunta && e.respuesta === text);
        if (!yaExiste) {
          learned.push({ pregunta, respuesta: text, item_id: item_id || null, item_title: item_title || null, tipo: 'pregunta', fecha: new Date().toISOString() });
          if (learned.length > 2000) learned = learned.slice(-2000);
          fs.writeFileSync(LEARNED_FILE, JSON.stringify(learned, null, 2));
          if (item_id && fs.existsSync(PREGUNTAS_FILE)) {
            const data = JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8'));
            if (!data.byPub[item_id]) data.byPub[item_id] = { titulo: item_title || item_id, qa: [] };
            const yaEnPub = data.byPub[item_id].qa.some(e => e.q === pregunta);
            if (!yaEnPub) {
              data.byPub[item_id].qa.push({ q: pregunta, a: text, aprendida: true });
              fs.writeFileSync(PREGUNTAS_FILE, JSON.stringify(data));
            }
          }
          console.log(`[auto-learn] guardado: "${pregunta.slice(0, 50)}..."`);
        }
      } catch(e) { console.error('[auto-learn]', e.message); }
    }
    res.json({ ok: true, data: r.data });
  } catch(e) {
    const detail = e.response?.data || e.message;
    console.error('[responder-ml]', detail);
    res.status(e.response?.status || 500).json({ error: detail });
  }
});

// POST /api/ml/preguntas/importar-historial
let importState = { running: false, progress: 0, total: 0, importadas: 0, error: null };
app.get('/api/ml/preguntas/importar-estado', requireToken, (req, res) => res.json(importState));

app.post('/api/ml/preguntas/importar-historial', requireToken, async (req, res) => {
  if (importState.running) return res.json({ ok: false, msg: 'ya corriendo' });
  importState = { running: true, progress: 0, total: 0, importadas: 0, error: null };
  res.json({ ok: true, msg: 'importación iniciada' });

  (async () => {
    try {
      let offset = 0;
      const limit = 50;
      let total = null;
      let learned = fs.existsSync(LEARNED_FILE) ? JSON.parse(fs.readFileSync(LEARNED_FILE, 'utf8')) : [];
      let preguntasData = fs.existsSync(PREGUNTAS_FILE) ? JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8')) : { byPub: {} };
      const existentes = new Set(learned.map(e => e.pregunta + '||' + e.respuesta));
      let importadas = 0;

      while (true) {
        const r = await axios.get(`${ML_API_URL}/my/received_questions/search`, {
          params: { status: 'ANSWERED', limit, offset },
          headers: { Authorization: `Bearer ${tokenData.access_token}` }
        });
        const qs = r.data.questions || [];
        if (total === null) { total = r.data.total || 0; importState.total = total; }
        if (!qs.length) break;

        for (const q of qs) {
          const answer = q.answer?.text;
          if (!q.text || !answer) continue;
          const key = q.text + '||' + answer;
          if (existentes.has(key)) continue;
          existentes.add(key);
          learned.push({
            pregunta: q.text,
            respuesta: answer,
            item_id: q.item_id || null,
            item_title: null,
            tipo: 'pregunta',
            fecha: q.date_created || new Date().toISOString()
          });
          if (q.item_id) {
            if (!preguntasData.byPub[q.item_id]) preguntasData.byPub[q.item_id] = { titulo: q.item_id, qa: [] };
            const yaEnPub = preguntasData.byPub[q.item_id].qa.some(e => e.q === q.text);
            if (!yaEnPub) preguntasData.byPub[q.item_id].qa.push({ q: q.text, a: answer });
          }
          importadas++;
        }

        offset += qs.length;
        importState.progress = offset;
        importState.importadas = importadas;
        if (offset >= total) break;
        await sleep(300);
      }

      if (learned.length > 5000) learned = learned.slice(-5000);
      fs.writeFileSync(LEARNED_FILE, JSON.stringify(learned, null, 2));
      fs.writeFileSync(PREGUNTAS_FILE, JSON.stringify(preguntasData));
      importState = { running: false, progress: offset, total, importadas, error: null };
      console.log(`[importar-historial] importadas ${importadas} preguntas nuevas`);
    } catch(e) {
      importState = { running: false, progress: importState.progress, total: importState.total, importadas: importState.importadas, error: e.message };
      console.error('[importar-historial]', e.message);
    }
  })();
});

// GET /api/ml/preguntas/stats
app.get('/api/ml/preguntas/stats', requireToken, (req, res) => {
  try {
    if (!fs.existsSync(PREGUNTAS_FILE)) return res.json({ pubs: [], total: 0 });
    const data = JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8'));
    const pubs = Object.entries(data.byPub).map(([id, p]) => ({
      id,
      titulo: p.titulo,
      total: p.qa.length,
      categorias: p.categorias || {}
    })).sort((a, b) => b.total - a.total);
    res.json({ pubs, total: pubs.reduce((s, p) => s + p.total, 0) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// GET /api/ml/preguntas/:itemId
app.get('/api/ml/preguntas/:itemId', requireToken, (req, res) => {
  try {
    if (!fs.existsSync(PREGUNTAS_FILE)) return res.json({ qa: [] });
    const data = JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8'));
    const pub  = data.byPub[req.params.itemId];
    if (!pub) return res.json({ qa: [], titulo: '' });
    res.json({ qa: pub.qa, titulo: pub.titulo });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/ml/preguntas/responder
app.post('/api/ml/preguntas/responder', requireToken, async (req, res) => {
  if (!anthropic) return res.status(400).json({ error: 'ANTHROPIC_API_KEY no configurada' });
  const { pregunta, itemId, titulo } = req.body;
  if (!pregunta) return res.status(400).json({ error: 'pregunta requerida' });

  try {
    let kb = null;
    if (fs.existsSync(QA_KB_FILE)) kb = JSON.parse(fs.readFileSync(QA_KB_FILE, 'utf8'));

    let ejemplosPub = [];
    if (itemId && fs.existsSync(PREGUNTAS_FILE)) {
      const data = JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8'));
      const pub  = data.byPub[itemId];
      if (pub) ejemplosPub = pub.qa.slice(-10);
    }

    const similares = buscarSimilares(pregunta, 6);

    const kbText = kb ? `
Estilo de respuesta:
- Saludo: "${kb.estilo.saludo}"
- Despedida: "${kb.estilo.despedida}"
- Tono: ${kb.estilo.tono}

Reglas:
${kb.reglas_generales.slice(0, 8).map(r => '- ' + r).join('\n')}
` : '';

    const ejemplosPubText = ejemplosPub.length ? `
Ejemplos anteriores para esta publicación:
${ejemplosPub.slice(0, 5).map(e => `P: ${e.q}\nR: ${e.a}`).join('\n---\n')}
` : '';

    const similoresText = similares.length ? `
Respuestas validadas similares (de otras publicaciones):
${similares.map(e => `P: ${e.pregunta}\nR: ${e.respuesta}`).join('\n---\n')}
` : '';

    const ejemplosText = ejemplosPubText + similoresText;

    const itemCtx = itemId ? await fetchItemContext(itemId) : null;
    const itemText = itemCtx ? buildItemContextText(itemCtx) : `Producto: ${titulo || 'no especificado'}`;

    const r = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 300,
      messages: [{
        role: 'user',
        content: `Sos el asistente de MUNDO SHOP en Mercado Libre Uruguay. Responde la siguiente pregunta de un comprador.
${kbText}${ejemplosText}${(() => { const r = loadReglasNegocio(); return r.length ? '\nInformación del negocio:\n' + r.map(x => `- ${x.categoria ? '['+x.categoria+'] ' : ''}${x.texto}`).join('\n') : ''; })()}
${itemText}

Pregunta del comprador: "${pregunta}"

Responde SOLO con el texto de la respuesta, sin explicaciones adicionales. Si no sabes un dato específico, no lo inventes — decí que lo consulten por el chat de la compra.`
      }]
    });

    res.json({ respuesta: r.content[0].text.trim() });
  } catch(e) {
    console.error('[preguntas/responder]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── Recomendaciones de publicaciones ─────────────────────────────

// GET /api/ml/recomendaciones/publicaciones
app.get('/api/ml/recomendaciones/publicaciones', requireToken, (req, res) => {
  try {
    if (!fs.existsSync(PREGUNTAS_FILE)) return res.json({ pubs: [] });
    const data = JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8'));
    const itemMap = {};
    cachedItems.forEach(i => { itemMap[i.id] = i; });

    const pubs = Object.entries(data.byPub).map(([id, p]) => {
      const cached = itemMap[id] || {};
      return {
        id,
        titulo: p.titulo || cached.title || id,
        thumbnail: cached.thumbnail || '',
        permalink: cached.permalink || '',
        total_preguntas: p.qa.length
      };
    }).sort((a, b) => b.total_preguntas - a.total_preguntas);

    res.json({ pubs });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// POST /api/ml/recomendaciones/analizar
app.post('/api/ml/recomendaciones/analizar', requireToken, async (req, res) => {
  if (!anthropic) return res.status(400).json({ error: 'ANTHROPIC_API_KEY no configurada' });
  const { itemId } = req.body;
  if (!itemId) return res.status(400).json({ error: 'itemId requerido' });

  try {
    const headers = { Authorization: `Bearer ${tokenData.access_token}` };

    // Fetch item context and preguntas in parallel
    const [itemCtx, preguntasData] = await Promise.all([
      fetchItemContext(itemId),
      fs.existsSync(PREGUNTAS_FILE)
        ? Promise.resolve(JSON.parse(fs.readFileSync(PREGUNTAS_FILE, 'utf8')))
        : Promise.resolve({ byPub: {} })
    ]);

    const pub = preguntasData.byPub[itemId];
    const qa = pub?.qa || [];

    // Buscar órdenes recientes del item para cruzar con reclamos
    let reclamosDelItem = [];
    try {
      const ordersR = await axios.get(`${ML_API_URL}/orders/search`, {
        headers,
        params: { seller: tokenData.user_id, item: itemId, limit: 50 }
      });
      const orderIds = new Set((ordersR.data.results || []).map(o => String(o.id)));
      if (orderIds.size > 0) {
        reclamosDelItem = cachedClaims.filter(c => orderIds.has(String(c.resource_id)));
      }
    } catch(_) {}

    if (qa.length === 0 && reclamosDelItem.length === 0) {
      return res.json({ recomendaciones: 'No hay preguntas ni reclamos suficientes para analizar esta publicación.' });
    }

    const itemText = buildItemContextText(itemCtx);
    const preguntasText = qa.length
      ? `Historial de preguntas de compradores (${qa.length} en total):\n` +
        qa.map((e, i) => `${i + 1}. P: ${e.q}${e.a ? `\n   R: ${e.a}` : ''}`).join('\n')
      : 'Sin preguntas históricas.';

    const reclamosText = reclamosDelItem.length
      ? `\nProblemas de postventa detectados (${reclamosDelItem.length} reclamos/devoluciones):\n` +
        reclamosDelItem.map((c, i) => {
          const tipo = c.type === 'return' ? 'Devolución' : 'Reclamo';
          const motivo = c.reason_id || c.sub_status || c.status || 'sin motivo';
          const resolucion = c.resolution?.reason || '';
          return `${i + 1}. [${tipo}] Motivo: ${motivo}${resolucion ? ` | Resolución: ${resolucion}` : ''}`;
        }).join('\n')
      : '\nSin reclamos ni devoluciones registradas.';

    const r = await anthropic.messages.create({
      model: 'claude-sonnet-4-6',
      max_tokens: 1200,
      messages: [{
        role: 'user',
        content: `Sos un experto en optimización de publicaciones de MercadoLibre Uruguay. Analizá las preguntas frecuentes y los problemas de postventa de esta publicación para generar recomendaciones concretas que mejoren la conversión y reduzcan problemas.

Datos de la publicación:
${itemText}

${preguntasText}
${reclamosText}

Respondé con:
1. **Temas más consultados**: qué preguntan más los compradores (agrupado por tema)
2. **Problemas de postventa**: patrones en reclamos o devoluciones y sus posibles causas
3. **Qué falta en la publicación**: información que debería estar en la ficha pero no está
4. **Recomendaciones concretas**: cambios específicos al título, descripción o fotos

Sé directo y específico. Máximo 500 palabras.`
      }]
    });

    res.json({ recomendaciones: r.content[0].text.trim() });
  } catch(e) {
    console.error('[recomendaciones/analizar]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ── Static files (ya montado arriba) ──

app.listen(PORT, () => {
  console.log(`ml-atencion corriendo en http://localhost:${PORT}`);
});
