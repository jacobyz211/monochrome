const express = require('express');
const cors    = require('cors');
const axios   = require('axios');
const crypto  = require('crypto');
const Redis   = require('ioredis');

const app  = express();
const PORT = process.env.PORT || 3000;
app.use(cors());
app.use(express.json());

const HIFI_INSTANCES = [
  'https://ohio-1.monochrome.tf',
  'https://frankfurt-1.monochrome.tf',
  'https://eu-central.monochrome.tf',
  'https://us-west.monochrome.tf',
  'https://hifi.geeked.wtf',
  'https://monochrome-api.samidy.com'
];
let activeInstance  = HIFI_INSTANCES[0];
let instanceHealthy = false;
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36';

// ─── Helpers ──────────────────────────────────────────────────────────────────
function coverUrl(uuid, size) {
  if (!uuid) return undefined;
  var s = String(uuid);
  if (s.startsWith('http')) return s;
  size = size || 320;
  return 'https://resources.tidal.com/images/' + s.replace(/-/g, '/') + '/' + size + 'x' + size + '.jpg';
}
function trackDuration(t) { return (t && t.duration) ? Math.floor(t.duration) : undefined; }
function trackArtist(t) {
  if (!t) return 'Unknown';
  if (t.artists && t.artists.length) return t.artists.map(function(a) { return a.name; }).join(', ');
  if (t.artist  && t.artist.name)   return t.artist.name;
  return 'Unknown';
}
function decodeManifest(manifest) {
  try {
    var decoded = JSON.parse(Buffer.from(manifest, 'base64').toString('utf8'));
    return { url: (decoded.urls && decoded.urls[0]) || null, codec: decoded.codecs || decoded.mimeType || '' };
  } catch (e) { return null; }
}
function isPlaylistUUID(id) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(String(id || ''));
}
function looksLikePlaylist(p) {
  if (!p || !p.title) return false;
  if (p.trackNumber !== undefined) return false;
  if (p.replayGain  !== undefined) return false;
  if (p.peak        !== undefined) return false;
  if (p.isrc        !== undefined) return false;
  if (p.audioQuality !== undefined) return false;
  return !!(p.uuid || p.creator || p.squareImage || p.numberOfTracks !== undefined);
}
function artistRelevance(name, query) {
  var n = (name  || '').toLowerCase().trim();
  var q = (query || '').toLowerCase().trim();
  if (n === q)                              return 4;
  if (n.startsWith(q) || q.startsWith(n))  return 3;
  if (n.includes(q)   || q.includes(n))    return 2;
  return 0;
}

// ─── Hi-Fi API client (shared pool) ──────────────────────────────────────────
async function hifiGet(path, params) {
  var errors    = [];
  var instances = instanceHealthy
    ? [activeInstance].concat(HIFI_INSTANCES.filter(function(i) { return i !== activeInstance; }))
    : HIFI_INSTANCES.slice();
  for (var inst of instances) {
    try {
      var r = await axios.get(inst + path, {
        params:  params || {},
        headers: { 'User-Agent': UA, 'Accept': 'application/json' },
        timeout: 15000
      });
      if (r.status === 200 && r.data) {
        if (inst !== activeInstance) { activeInstance = inst; instanceHealthy = true; console.log('[hifi] switched to ' + inst); }
        return r.data;
      }
    } catch (e) { errors.push(inst + ': ' + e.message); }
  }
  throw new Error('All Hi-Fi instances failed: ' + errors.slice(-2).join(' | '));
}
async function hifiGetSafe(path, params) {
  try { return await hifiGet(path, params); } catch (e) { return null; }
}

// ─── Hi-Fi API client (token-bound instance) ─────────────────────────────────
async function hifiGetForToken(instanceUrl, path, params) {
  if (instanceUrl) {
    try {
      var r = await axios.get(instanceUrl + path, {
        params:  params || {},
        headers: { 'User-Agent': UA, 'Accept': 'application/json' },
        timeout: 15000
      });
      if (r.status === 200 && r.data) return r.data;
      throw new Error('Non-200 from custom instance: ' + r.status);
    } catch (e) {
      throw new Error('Custom instance failed (' + instanceUrl + '): ' + e.message);
    }
  }
  return hifiGet(path, params);
}
async function hifiGetForTokenSafe(instanceUrl, path, params) {
  try { return await hifiGetForToken(instanceUrl, path, params); } catch (e) { return null; }
}

async function checkInstances() {
  for (var inst of HIFI_INSTANCES) {
    try {
      await axios.get(inst + '/search/', { params: { s: 'test', limit: 1 }, timeout: 8000 });
      activeInstance = inst; instanceHealthy = true; console.log('[hifi] healthy: ' + inst); return;
    } catch (e) {}
  }
  instanceHealthy = false; console.warn('[hifi] WARNING: no healthy instances.');
}
checkInstances();
setInterval(checkInstances, 15 * 60 * 1000);

// ─── Redis ────────────────────────────────────────────────────────────────────
let redis = null;
if (process.env.REDIS_URL) {
  redis = new Redis(process.env.REDIS_URL, { maxRetriesPerRequest: 3, enableReadyCheck: false });
  redis.on('connect', function() { console.log('[Redis] connected'); });
  redis.on('error',   function(e) { console.error('[Redis] ' + e.message); });
}
async function redisSave(token, entry) {
  if (!redis) return;
  try {
    await redis.set('mc:token:' + token, JSON.stringify({
      createdAt:        entry.createdAt,
      lastUsed:         entry.lastUsed,
      reqCount:         entry.reqCount,
      instanceUrl:      entry.instanceUrl      || null,
      preferredQuality: entry.preferredQuality || null   // ← new
    }));
  } catch (e) {}
}
async function redisLoad(token) {
  if (!redis) return null;
  try {
    var d = await redis.get('mc:token:' + token);
    if (!d) return null;
    var p = JSON.parse(d);
    return {
      createdAt:        p.createdAt,
      lastUsed:         p.lastUsed,
      reqCount:         p.reqCount,
      instanceUrl:      p.instanceUrl      || null,
      preferredQuality: p.preferredQuality || null   // ← new
    };
  } catch (e) { return null; }
}

// ─── Token auth ───────────────────────────────────────────────────────────────
const TOKEN_CACHE = new Map(), IP_CREATES = new Map();
const MAX_TOKENS_PER_IP = 10, RATE_MAX = 80, RATE_WINDOW_MS = 60000;
function generateToken() { return crypto.randomBytes(14).toString('hex'); }
function getOrCreateIpBucket(ip) {
  var now = Date.now(), b = IP_CREATES.get(ip);
  if (!b || now > b.resetAt) { b = { count: 0, resetAt: now + 86400000 }; IP_CREATES.set(ip, b); }
  return b;
}
async function getTokenEntry(token) {
  if (TOKEN_CACHE.has(token)) return TOKEN_CACHE.get(token);
  var saved = await redisLoad(token);
  if (!saved) return null;
  var entry = {
    createdAt:        saved.createdAt,
    lastUsed:         saved.lastUsed,
    reqCount:         saved.reqCount,
    instanceUrl:      saved.instanceUrl      || null,
    preferredQuality: saved.preferredQuality || null,  // ← new
    rateWin:          []
  };
  TOKEN_CACHE.set(token, entry);
  return entry;
}
function checkRateLimit(entry) {
  var now = Date.now();
  entry.rateWin = (entry.rateWin || []).filter(function(t) { return now - t < RATE_WINDOW_MS; });
  if (entry.rateWin.length >= RATE_MAX) return false;
  entry.rateWin.push(now); entry.lastUsed = now; entry.reqCount = (entry.reqCount || 0) + 1; return true;
}
async function tokenMiddleware(req, res, next) {
  var entry = await getTokenEntry(req.params.token);
  if (!entry)                return res.status(404).json({ error: 'Invalid token.' });
  if (!checkRateLimit(entry)) return res.status(429).json({ error: 'Rate limit exceeded.' });
  req.tokenEntry = entry;
  if (entry.reqCount % 20 === 0) redisSave(req.params.token, entry);
  next();
}
function getBaseUrl(req) { return (req.headers['x-forwarded-proto'] || req.protocol) + '://' + req.get('host'); }

// ─── Config page ──────────────────────────────────────────────────────────────
function buildConfigPage(baseUrl) {
  var h = '';
  h += '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">';
  h += '<meta name="viewport" content="width=device-width,initial-scale=1">';
  h += '<title>Claudochrome - TIDAL Addon</title>';
  h += '<style>*{box-sizing:border-box;margin:0;padding:0}';
  h += 'body{background:#080808;color:#e0e0e0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:48px 20px 64px}';
  h += '.card{background:#111;border:1px solid #1e1e1e;border-radius:18px;padding:36px;max-width:540px;width:100%;box-shadow:0 24px 64px rgba(0,0,0,.6);margin-bottom:20px}';
  h += 'h1{font-size:22px;font-weight:700;margin-bottom:6px;color:#fff}h2{font-size:16px;font-weight:700;margin-bottom:14px;color:#fff}';
  h += 'p.sub{font-size:14px;color:#666;margin-bottom:20px;line-height:1.6}';
  h += '.tip{background:#0a0a0a;border:1px solid #1e1e1e;border-radius:10px;padding:12px 14px;margin-bottom:20px;font-size:12px;color:#888;line-height:1.7}.tip b{color:#ccc}';
  h += '.pills{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:24px}';
  h += '.pill{border-radius:20px;font-size:11px;font-weight:600;padding:4px 10px;background:#181818;color:#aaa;border:1px solid #2a2a2a}';
  h += '.pill.hi{background:#0d1520;color:#4a9eff;border-color:#1a3050}';
  h += '.lbl{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:#444;margin-bottom:8px;margin-top:16px}';
  h += 'input{width:100%;background:#0a0a0a;border:1px solid #1e1e1e;border-radius:10px;color:#e0e0e0;font-size:14px;padding:12px 14px;margin-bottom:6px;outline:none;transition:border-color .15s}';
  h += 'input:focus{border-color:#fff}input::placeholder{color:#2e2e2e}';
  h += '.hint{font-size:12px;color:#3a3a3a;margin-bottom:12px;line-height:1.7}';
  h += 'button{cursor:pointer;border:none;border-radius:10px;font-size:15px;font-weight:700;padding:13px;width:100%;margin-top:6px;margin-bottom:12px;transition:background .15s}';
  h += '.bw{background:#fff;color:#000}.bw:hover{background:#e0e0e0}.bw:disabled{background:#1e1e1e;color:#333;cursor:not-allowed}';
  h += '.bg{background:#141414;color:#e0e0e0;border:1px solid #2a2a2a}.bg:hover{background:#1e1e1e}.bg:disabled{background:#0f0f0f;color:#333;cursor:not-allowed}';
  h += '.bd{background:#0f0f0f;color:#777;border:1px solid #1a1a1a;font-size:13px;padding:10px}.bd:hover{background:#1a1a1a;color:#fff}';
  h += '.box{display:none;background:#0a0a0a;border:1px solid #1a1a1a;border-radius:12px;padding:18px;margin-bottom:14px}';
  h += '.blbl{font-size:10px;color:#444;text-transform:uppercase;letter-spacing:.07em;margin-bottom:8px}';
  h += '.burl{font-size:12px;color:#fff;word-break:break-all;font-family:"SF Mono","Fira Code",monospace;margin-bottom:14px;line-height:1.5}';
  h += 'hr{border:none;border-top:1px solid #161616;margin:24px 0}';
  h += '.steps{display:flex;flex-direction:column;gap:12px}.step{display:flex;gap:12px;align-items:flex-start}';
  h += '.sn{background:#161616;border:1px solid #222;border-radius:50%;width:26px;height:26px;min-width:26px;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:#555}';
  h += '.st{font-size:13px;color:#555;line-height:1.6}.st b{color:#999}';
  h += '.warn{background:#0d0d0d;border:1px solid #1e1e1e;border-radius:10px;padding:14px;margin-top:20px;font-size:12px;color:#555;line-height:1.7}';
  h += '.inst-list{display:flex;flex-direction:column;gap:6px;margin-top:10px}';
  h += '.inst{display:flex;align-items:center;gap:8px;font-size:12px;padding:8px 12px;background:#0a0a0a;border:1px solid #161616;border-radius:8px}';
  h += '.dot{width:7px;height:7px;border-radius:50%;background:#333;flex-shrink:0}.dot.ok{background:#4a9a4a}.dot.err{background:#c04040}';
  h += '.inst-url{flex:1;color:#666;font-family:"SF Mono","Fira Code",monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}';
  h += '.inst-ms{color:#444;margin-left:auto;font-size:11px}';
  h += '.badge{display:none;background:#0d1a0d;border:1px solid #1a3a1a;border-radius:8px;padding:8px 12px;font-size:12px;color:#4a9a4a;margin-bottom:10px}';

  // ── Quality selector styles ────────────────────────────────────────────────
  h += '.ql-row{display:flex;gap:8px;margin-bottom:6px}';
  h += '.ql-btn{flex:1;cursor:pointer;border:1px solid #2a2a2a;border-radius:10px;background:#0a0a0a;color:#555;font-size:12px;font-weight:700;padding:10px 6px;text-align:center;transition:all .15s;letter-spacing:.04em}';
  h += '.ql-btn:hover{border-color:#444;color:#aaa}';
  h += '.ql-btn.sel{background:#0d1520;border-color:#4a9eff;color:#4a9eff}';

  h += 'footer{margin-top:32px;font-size:12px;color:#2a2a2a;text-align:center;line-height:1.8}</style></head><body>';

  h += '<svg width="52" height="52" viewBox="0 0 52 52" fill="none" style="margin-bottom:22px"><circle cx="26" cy="26" r="26" fill="#fff"/><rect x="10" y="20" width="4" height="12" rx="2" fill="#000"/><rect x="17" y="14" width="4" height="24" rx="2" fill="#000"/><rect x="24" y="18" width="4" height="16" rx="2" fill="#000"/><rect x="31" y="11" width="4" height="30" rx="2" fill="#000"/><rect x="38" y="17" width="4" height="18" rx="2" fill="#000"/></svg>';

  h += '<div class="card"><h1>Claudochrome for Eclipse</h1>';
  h += '<p class="sub">Full TIDAL catalog — lossless FLAC, HiRes, AAC 320 — no account, no subscription.</p>';
  h += '<div class="tip"><b>Save your URL.</b> Paste it below to refresh without reinstalling.</div>';
  h += '<div class="pills"><span class="pill">Tracks &middot; Albums &middot; Artists</span><span class="pill hi">FLAC / HiRes</span><span class="pill hi">AAC 320</span></div>';

  h += '<div class="lbl">Custom Hi&#8209;Fi Instance <span style="color:#2a2a2a;font-weight:400;text-transform:none">(optional)</span></div>';
  h += '<input type="text" id="customInstance" placeholder="https://your-instance.example.com">';
  h += '<div class="hint">Leave blank to use the shared pool. Paste your own self-hosted Hi-Fi API URL to lock this token exclusively to your instance.</div>';

  // ── Quality selector ───────────────────────────────────────────────────────
  h += '<div class="lbl">Preferred Audio Quality <span style="color:#2a2a2a;font-weight:400;text-transform:none">(optional)</span></div>';
  h += '<div class="ql-row">';
  h += '<div class="ql-btn" id="ql-LOSSLESS" onclick="selectQuality(\'LOSSLESS\')">LOSSLESS<br><span style="font-size:10px;font-weight:400;color:inherit;opacity:.6">FLAC / HiRes</span></div>';
  h += '<div class="ql-btn" id="ql-HIGH"     onclick="selectQuality(\'HIGH\')"    >HIGH<br><span style="font-size:10px;font-weight:400;color:inherit;opacity:.6">AAC 320 kbps</span></div>';
  h += '<div class="ql-btn" id="ql-LOW"      onclick="selectQuality(\'LOW\')"     >LOW<br><span style="font-size:10px;font-weight:400;color:inherit;opacity:.6">AAC 128 kbps</span></div>';
  h += '</div>';
  h += '<div class="hint" id="qlHint">No preference — addon tries LOSSLESS &rarr; HIGH &rarr; LOW automatically.</div>';

  h += '<button class="bw" id="genBtn" onclick="generate()">Generate My Addon URL</button>';
  h += '<div class="box" id="genBox"><div class="badge" id="genBadge">&#10003; Locked to your custom instance</div><div class="blbl">Your addon URL &mdash; paste into Eclipse</div><div class="burl" id="genUrl"></div><button class="bd" id="copyGenBtn" onclick="copyGen()">Copy URL</button></div>';

  h += '<hr><div class="lbl">Refresh existing URL</div>';
  h += '<input type="text" id="existingUrl" placeholder="Paste your existing addon URL here">';
  h += '<div class="hint">Keeps the same URL active &mdash; nothing to reinstall.</div>';
  h += '<button class="bg" id="refBtn" onclick="doRefresh()">Refresh Existing URL</button>';
  h += '<div class="box" id="refBox"><div class="blbl">Refreshed &mdash; same URL still works in Eclipse</div><div class="burl" id="refUrl"></div><button class="bd" id="copyRefBtn" onclick="copyRef()">Copy URL</button></div>';

  h += '<hr><div class="steps">';
  h += '<div class="step"><div class="sn">1</div><div class="st">Generate and copy your URL above</div></div>';
  h += '<div class="step"><div class="sn">2</div><div class="st">Open <b>Eclipse</b> &rarr; Settings &rarr; Connections &rarr; Add Connection &rarr; Addon</div></div>';
  h += '<div class="step"><div class="sn">3</div><div class="st">Paste your URL and tap Install</div></div>';
  h += '<div class="step"><div class="sn">4</div><div class="st">Search TIDAL\'s full catalog &mdash; FLAC quality auto-selected</div></div>';
  h += '</div><div class="warn">Hi-Fi instances are community-hosted. The addon auto-discovers working instances and fails over automatically. Tokens locked to a custom instance will only use that instance.</div></div>';

  h += '<div class="card"><h2>Instance Health</h2>';
  h += '<p class="sub" style="margin-bottom:14px">Live status of all Hi-Fi API v2.7 instances.</p>';
  h += '<div class="inst-list" id="instList"><div style="color:#333;font-size:13px">Checking...</div></div>';
  h += '<button class="bg" style="margin-top:14px" onclick="checkHealth()">Refresh Status</button></div>';
  h += '<footer>Claudochrome Eclipse Addon v2.0.0 &bull; Hi-Fi API v2.7</footer>';

  h += '<script>';
  h += 'var _gu="",_ru="",_selQ=null;';

  // quality toggle — clicking the same pill again deselects it
  h += 'var QLABELS={LOSSLESS:"LOSSLESS (FLAC / HiRes)",HIGH:"HIGH (AAC 320 kbps)",LOW:"LOW (AAC 128 kbps)"};';
  h += 'function selectQuality(q){';
  h += '  if(_selQ===q){_selQ=null;}else{_selQ=q;}';
  h += '  ["LOSSLESS","HIGH","LOW"].forEach(function(k){';
  h += '    document.getElementById("ql-"+k).classList.toggle("sel",_selQ===k);';
  h += '  });';
  h += '  document.getElementById("qlHint").textContent=_selQ';
  h += '    ? "Preferred: "+QLABELS[_selQ]+" \u2014 fallback to lower qualities if unavailable."';
  h += '    : "No preference \u2014 addon tries LOSSLESS \u2192 HIGH \u2192 LOW automatically.";';
  h += '}';

  h += 'function generate(){';
  h += '  var btn=document.getElementById("genBtn");btn.disabled=true;btn.textContent="Generating...";';
  h += '  var ci=document.getElementById("customInstance").value.trim();';
  h += '  while(ci.length&&ci[ci.length-1]==="/")ci=ci.slice(0,-1);';
  h += '  var body={};';
  h += '  if(ci)body.instanceUrl=ci;';
  h += '  if(_selQ)body.preferredQuality=_selQ;';   // ← send quality if chosen
  h += '  fetch("/generate",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)})';
  h += '    .then(function(r){return r.json();})';
  h += '    .then(function(d){';
  h += '      if(d.error){alert(d.error);btn.disabled=false;btn.textContent="Generate My Addon URL";return;}';
  h += '      _gu=d.manifestUrl;';
  h += '      document.getElementById("genUrl").textContent=_gu;';
  h += '      document.getElementById("genBadge").style.display=d.usingCustomInstance?"block":"none";';
  h += '      document.getElementById("genBox").style.display="block";';
  h += '      btn.disabled=false;btn.textContent="Regenerate URL";';
  h += '    }).catch(function(e){alert("Error: "+e.message);btn.disabled=false;btn.textContent="Generate My Addon URL";});';
  h += '}';

  h += 'function copyGen(){if(!_gu)return;navigator.clipboard.writeText(_gu).then(function(){var b=document.getElementById("copyGenBtn");b.textContent="Copied!";setTimeout(function(){b.textContent="Copy URL";},1500);});}';

  h += 'function doRefresh(){';
  h += '  var btn=document.getElementById("refBtn");';
  h += '  var eu=document.getElementById("existingUrl").value.trim();';
  h += '  if(!eu){alert("Paste your existing addon URL first.");return;}';
  h += '  btn.disabled=true;btn.textContent="Refreshing...";';
  h += '  fetch("/refresh",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({existingUrl:eu})})';
  h += '    .then(function(r){return r.json();})';
  h += '    .then(function(d){';
  h += '      if(d.error){alert(d.error);btn.disabled=false;btn.textContent="Refresh Existing URL";return;}';
  h += '      _ru=d.manifestUrl;document.getElementById("refUrl").textContent=_ru;';
  h += '      document.getElementById("refBox").style.display="block";';
  h += '      btn.disabled=false;btn.textContent="Refresh Again";';
  h += '    }).catch(function(e){alert("Error: "+e.message);btn.disabled=false;btn.textContent="Refresh Existing URL";});';
  h += '}';

  h += 'function copyRef(){if(!_ru)return;navigator.clipboard.writeText(_ru).then(function(){var b=document.getElementById("copyRefBtn");b.textContent="Copied!";setTimeout(function(){b.textContent="Copy URL";},1500);});}';

  h += 'function checkHealth(){var list=document.getElementById("instList");list.innerHTML="<div style=\\"color:#333;font-size:13px\\">Checking...</div>";';
  h += 'fetch("/instances").then(function(r){return r.json();}).then(function(data){list.innerHTML="";(data.instances||[]).forEach(function(inst){var row=document.createElement("div");row.className="inst";var dot=document.createElement("span");dot.className=inst.ok?"dot ok":"dot err";var urlSpan=document.createElement("span");urlSpan.className="inst-url";urlSpan.textContent=inst.url;row.appendChild(dot);row.appendChild(urlSpan);if(inst.ok){var ms=document.createElement("span");ms.className="inst-ms";ms.textContent=inst.ms+"ms";row.appendChild(ms);}list.appendChild(row);});}).catch(function(){list.innerHTML="<div style=\\"color:#c04040;font-size:13px\\">Could not reach server</div>";});}';
  h += 'checkHealth();';
  h += '<\/script></body></html>';
  return h;
}

// ─── Public routes ────────────────────────────────────────────────────────────
app.get('/', function(req, res) { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.send(buildConfigPage(getBaseUrl(req))); });

app.post('/generate', async function(req, res) {
  var ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown').split(',')[0].trim();
  var bucket = getOrCreateIpBucket(ip);
  if (bucket.count >= MAX_TOKENS_PER_IP) return res.status(429).json({ error: 'Too many tokens from this IP today.' });

  var instanceUrl = (req.body && req.body.instanceUrl) ? String(req.body.instanceUrl).trim().replace(/\/+$/, '') : null;
  if (instanceUrl) {
    if (!/^https?:\/\/.+/.test(instanceUrl))
      return res.status(400).json({ error: 'Instance URL must start with http:// or https://' });
    try {
      await axios.get(instanceUrl + '/search/', { params: { s: 'test', limit: 1 }, timeout: 8000 });
    } catch (e) {
      return res.status(400).json({ error: 'Could not reach your instance: ' + e.message });
    }
  }

  // ── Preferred quality ──────────────────────────────────────────────────────
  var VALID_QUALITIES = ['LOSSLESS', 'HIGH', 'LOW'];
  var preferredQuality = (req.body && req.body.preferredQuality && VALID_QUALITIES.includes(req.body.preferredQuality))
    ? req.body.preferredQuality
    : null;

  var token = generateToken();
  var entry = {
    createdAt:        Date.now(),
    lastUsed:         Date.now(),
    reqCount:         0,
    rateWin:          [],
    instanceUrl:      instanceUrl,
    preferredQuality: preferredQuality   // ← new
  };
  TOKEN_CACHE.set(token, entry);
  await redisSave(token, entry);
  bucket.count++;

  res.json({
    token:               token,
    manifestUrl:         getBaseUrl(req) + '/u/' + token + '/manifest.json',
    usingCustomInstance: !!instanceUrl,
    preferredQuality:    preferredQuality   // ← echoed back to UI
  });
});

app.post('/refresh', async function(req, res) {
  var raw = (req.body && req.body.existingUrl) ? String(req.body.existingUrl).trim() : '';
  var token = raw, m = raw.match(/\/u\/([a-f0-9]{28})\//);
  if (m) token = m[1];
  if (!token || !/^[a-f0-9]{28}$/.test(token)) return res.status(400).json({ error: 'Paste your full addon URL.' });
  var entry = await getTokenEntry(token);
  if (!entry) return res.status(404).json({ error: 'URL not found. Generate a new one.' });
  res.json({ token: token, manifestUrl: getBaseUrl(req) + '/u/' + token + '/manifest.json', refreshed: true });
});

app.get('/instances', async function(_req, res) {
  var results = await Promise.all(HIFI_INSTANCES.map(async function(inst) {
    var start = Date.now();
    try { await axios.get(inst + '/search/', { params: { s: 'test', limit: 1 }, timeout: 6000 }); return { url: inst, ok: true, ms: Date.now() - start }; }
    catch (e) { return { url: inst, ok: false, ms: null }; }
  }));
  res.json({ instances: results });
});

app.get('/health', function(_req, res) {
  res.json({ status: 'ok', version: '2.0.0', activeInstance: activeInstance, instanceHealthy: instanceHealthy, activeTokens: TOKEN_CACHE.size, redisConnected: !!(redis && redis.status === 'ready'), timestamp: new Date().toISOString() });
});

// ─── Manifest ─────────────────────────────────────────────────────────────────
app.get('/u/:token/manifest.json', tokenMiddleware, function(req, res) {
  res.json({
    id:          'com.eclipse.claudochrome.' + req.params.token.slice(0, 8),
    name:        'Claudochrome',
    version:     '2.0.0',
    description: 'Full TIDAL catalog via Hi-Fi API v2.7. Lossless FLAC, AAC 320. No account required.',
    icon:        'https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSQe_DbvCgGyEcwqhFv8S-Y7ULHa-0FCSHlfJQqpB0CuQ&s=10',
    resources:   ['search', 'stream', 'catalog'],
    types:       ['track', 'album', 'artist', 'playlist']
  });
});

// ─── Search ───────────────────────────────────────────────────────────────────
app.get('/u/:token/search', tokenMiddleware, async function(req, res) {
  var q    = String(req.query.q || req.query.query || req.query.s || '').trim();
  var inst = req.tokenEntry.instanceUrl;
  console.log('[search] q:', JSON.stringify(q));
  if (!q) return res.json({ tracks: [], albums: [], artists: [], playlists: [] });

  try {
    var [mainResult, pl1, pl2, pl3, pl4, pl5, ar1, ar2, ar3] = await Promise.allSettled([
      hifiGetForToken(inst, '/search/', { s: q, limit: 20, offset: 0 }),
      hifiGetForTokenSafe(inst, '/search/', { s: q, type: 'PLAYLISTS',  limit: 10, offset: 0 }),
      hifiGetForTokenSafe(inst, '/search/', { s: q, types: 'PLAYLISTS', limit: 10, offset: 0 }),
      hifiGetForTokenSafe(inst, '/search/', { s: q, filter: 'PLAYLISTS', limit: 10, offset: 0 }),
      hifiGetForTokenSafe(inst, '/playlists/search/', { s: q, limit: 10, offset: 0 }),
      hifiGetForTokenSafe(inst, '/search/playlists/', { s: q, limit: 10, offset: 0 }),
      hifiGetForTokenSafe(inst, '/search/', { s: q, type: 'ARTISTS',  limit: 5, offset: 0 }),
      hifiGetForTokenSafe(inst, '/search/', { s: q, types: 'ARTISTS', limit: 5, offset: 0 }),
      hifiGetForTokenSafe(inst, '/search/artists/', { s: q, limit: 5, offset: 0 })
    ]);

    var data  = mainResult.status === 'fulfilled' ? mainResult.value : null;
    var items = (data && data.data && data.data.items) ? data.data.items : [];
    console.log('[search] got', items.length, 'items');

    var albumMap = {}, artistMap = {}, artistHits = {}, tracks = [];

    for (var i = 0; i < items.length; i++) {
      var t = items[i];
      if (!t || !t.id) continue;

      if (t.album && t.album.id) {
        var abid = String(t.album.id);
        if (!albumMap[abid]) albumMap[abid] = { id: abid, title: t.album.title || 'Unknown', artist: trackArtist(t), artworkURL: coverUrl(t.album.cover), trackCount: t.album.numberOfTracks || undefined, year: t.album.releaseDate ? String(t.album.releaseDate).slice(0, 4) : undefined };
      }

      var arts = t.artists || (t.artist ? [t.artist] : []);
      for (var j = 0; j < arts.length; j++) {
        var a = arts[j];
        if (a && a.id) {
          var arid = String(a.id);
          if (!artistMap[arid]) artistMap[arid] = { id: arid, name: a.name || 'Unknown', artworkURL: coverUrl(a.picture, 320) };
          artistHits[arid] = (artistHits[arid] || 0) + 1;
        }
      }

      if (t.streamReady === false || t.allowStreaming === false) continue;
      tracks.push({ id: String(t.id), title: t.title || 'Unknown', artist: trackArtist(t), album: (t.album && t.album.title) || undefined, duration: trackDuration(t), artworkURL: coverUrl(t.album && t.album.cover), format: 'flac' });
    }

    // ── Playlist extraction ───────────────────────────────────────────────────
    var plItems = [];
    var plCandidates = [pl1, pl2, pl3, pl4, pl5];
    var plLabels     = ['type=PLAYLISTS', 'types=PLAYLISTS', 'filter=PLAYLISTS', '/playlists/search/', '/search/playlists/'];

    for (var ci = 0; ci < plCandidates.length; ci++) {
      var candidate = plCandidates[ci];
      if (candidate.status !== 'fulfilled' || !candidate.value) continue;
      var raw = candidate.value;

      console.log('[playlist-search] endpoint "' + plLabels[ci] + '" top keys:', Object.keys(raw));

      var rawItems = [];
      if      (raw.data && raw.data.playlists && Array.isArray(raw.data.playlists.items)) rawItems = raw.data.playlists.items;
      else if (raw.data && raw.data.playlists && Array.isArray(raw.data.playlists))        rawItems = raw.data.playlists;
      else if (raw.data && raw.data.items && Array.isArray(raw.data.items))                rawItems = raw.data.items;
      else if (raw.playlists && Array.isArray(raw.playlists.items))                        rawItems = raw.playlists.items;
      else if (raw.playlists && Array.isArray(raw.playlists))                              rawItems = raw.playlists;
      else if (Array.isArray(raw.items))                                                   rawItems = raw.items;
      else if (Array.isArray(raw.data))                                                    rawItems = raw.data;

      if (rawItems.length > 0) {
        console.log('[playlist-search] "' + plLabels[ci] + '" returned', rawItems.length, 'items, first keys:', Object.keys(rawItems[0]));
      }

      var realPlaylists = rawItems.filter(looksLikePlaylist);
      console.log('[playlist-search] "' + plLabels[ci] + '" real playlists after filter:', realPlaylists.length);

      if (realPlaylists.length > 0) {
        plItems = realPlaylists.slice(0, 5).map(function(p) {
          return { id: String(p.uuid || p.id || ''), title: p.title || 'Playlist', creator: (p.creator && p.creator.name) || undefined, artworkURL: coverUrl(p.squareImage || p.image), trackCount: p.numberOfTracks || undefined };
        }).filter(function(p) { return p.id; });
        if (plItems.length > 0) break;
      }
    }

    var albumList = Object.keys(albumMap).map(function(k) { return albumMap[k]; }).slice(0, 8);

    // ── Artist list: score by name relevance + hit count, prefer dedicated endpoints
    var artistList = Object.keys(artistMap)
      .sort(function(a, b) {
        var scoreB = artistRelevance(artistMap[b].name, q) * 100 + (artistHits[b] || 0);
        var scoreA = artistRelevance(artistMap[a].name, q) * 100 + (artistHits[a] || 0);
        return scoreB - scoreA;
      })
      .slice(0, 5)
      .map(function(k) { return artistMap[k]; });

    // ── Try dedicated artist search endpoints to override track-derived list ──
    var arCandidates = [ar1, ar2, ar3];
    var arLabels     = ['type=ARTISTS', 'types=ARTISTS', '/search/artists/'];
    for (var ai = 0; ai < arCandidates.length; ai++) {
      var arRes = arCandidates[ai];
      if (arRes.status !== 'fulfilled' || !arRes.value) continue;
      var arRaw = arRes.value;

      var arItems = [];
      if      (arRaw.data && arRaw.data.artists && Array.isArray(arRaw.data.artists.items)) arItems = arRaw.data.artists.items;
      else if (arRaw.data && arRaw.data.artists && Array.isArray(arRaw.data.artists))        arItems = arRaw.data.artists;
      else if (arRaw.data && Array.isArray(arRaw.data.items))                                arItems = arRaw.data.items;
      else if (Array.isArray(arRaw.items))                                                   arItems = arRaw.items;
      else if (Array.isArray(arRaw.artists))                                                 arItems = arRaw.artists;

      var realArtists = arItems.filter(function(a) { return a && (a.id || a.artistId) && a.name; });
      console.log('[artist-search] "' + arLabels[ai] + '" real artists:', realArtists.length);

      if (realArtists.length > 0) {
        artistList = realArtists
          .sort(function(a, b) { return artistRelevance(b.name, q) - artistRelevance(a.name, q); })
          .slice(0, 5)
          .map(function(a) {
            return { id: String(a.id || a.artistId), name: a.name, artworkURL: coverUrl(a.picture || a.cover, 320) };
          });
        console.log('[search] using dedicated artist results:', artistList.length);
        break;
      }
    }

    // ── Artwork fallback: fetch top artist if artwork is missing ──────────────
    if (artistList.length > 0 && !artistList[0].artworkURL) {
      try {
        var topArtData = await hifiGetForTokenSafe(inst, '/artist/', { id: artistList[0].id });
        if (topArtData) {
          var topArt = topArtData.data || topArtData;
          var pic = (topArt.cover && topArt.cover['320']) || coverUrl(topArt.picture || topArt.image, 320);
          if (pic) artistList[0].artworkURL = pic;
        }
      } catch (_) {}
    }

    console.log('[search] returning tracks:', tracks.length, 'albums:', albumList.length, 'artists:', artistList.length, 'playlists:', plItems.length);
    res.json({ tracks: tracks, albums: albumList, artists: artistList, playlists: plItems });

  } catch (e) {
    console.error('[search] ERROR:', e.message);
    res.status(502).json({ error: 'Search failed: ' + e.message, tracks: [], albums: [], artists: [], playlists: [] });
  }
});

// ─── Stream ───────────────────────────────────────────────────────────────────
app.get('/u/:token/stream/:id', tokenMiddleware, async function(req, res) {
  var tid  = req.params.id;
  var inst = req.tokenEntry.instanceUrl;

  // Build quality order: preferred first, then the rest highest→lowest
  var ALL_QUALITIES = ['LOSSLESS', 'HIGH', 'LOW'];
  var pref = req.tokenEntry.preferredQuality;
  var qualities = pref
    ? [pref].concat(ALL_QUALITIES.filter(function(q) { return q !== pref; }))
    : ALL_QUALITIES;

  for (var qi = 0; qi < qualities.length; qi++) {
    // … rest of the loop is unchanged
    var ql = qualities[qi];
    try {
      var data = await hifiGetForToken(inst, '/track/', { id: tid, quality: ql });
      var payload = (data && data.data) ? data.data : data;
      if (payload && payload.manifest) {
        var decoded = decodeManifest(payload.manifest);
        if (decoded && decoded.url) {
          var isFlac = decoded.codec && (decoded.codec.indexOf('flac') !== -1 || decoded.codec.indexOf('audio/flac') !== -1);
          return res.json({ url: decoded.url, format: isFlac ? 'flac' : 'aac', quality: ql === 'LOSSLESS' ? 'lossless' : (ql === 'HIGH' ? '320kbps' : '128kbps'), expiresAt: Math.floor(Date.now() / 1000) + 21600 });
        }
      }
      if (payload && payload.url) return res.json({ url: payload.url, format: 'aac', quality: 'lossless', expiresAt: Math.floor(Date.now() / 1000) + 21600 });
    } catch (e) {
      if (qi === qualities.length - 1) { console.error('[stream] all qualities failed for ' + tid + ': ' + e.message); return res.status(502).json({ error: 'Could not get stream URL for track ' + tid }); }
    }
  }
  return res.status(404).json({ error: 'No stream found for track ' + tid });
});

// ─── Album ────────────────────────────────────────────────────────────────────
app.get('/u/:token/album/:id', tokenMiddleware, async function(req, res) {
  var aid  = req.params.id;
  var inst = req.tokenEntry.instanceUrl;
  try {
    var data = await hifiGetForToken(inst, '/album/', { id: aid, limit: 100, offset: 0 });
    var album = (data && data.data) ? data.data : data;
    var rawItems = album.items || [];
    var artistName = 'Unknown';
    if      (album.artist  && album.artist.name)   artistName = album.artist.name;
    else if (album.artists && album.artists.length) artistName = album.artists.map(function(a) { return a.name; }).join(', ');
    var tracks = [];
    for (var i = 0; i < rawItems.length; i++) {
      var t = rawItems[i].item || rawItems[i];
      if (!t || !t.id || t.streamReady === false) continue;
      tracks.push({ id: String(t.id), title: t.title || 'Unknown', artist: trackArtist(t) || artistName, duration: trackDuration(t), trackNumber: t.trackNumber || (i + 1), artworkURL: coverUrl(album.cover) });
    }
    res.json({ id: String(album.id || aid), title: album.title || 'Unknown', artist: artistName, artworkURL: coverUrl(album.cover, 640), year: (album.releaseDate || '').slice(0, 4) || undefined, trackCount: album.numberOfTracks || tracks.length, tracks: tracks });
  } catch (e) { console.error('[album] ' + e.message); res.status(502).json({ error: 'Album fetch failed: ' + e.message }); }
});

// ─── Artist ───────────────────────────────────────────────────────────────────
app.get('/u/:token/artist/:id', tokenMiddleware, async function(req, res) {
  var aid  = parseInt(req.params.id, 10);
  var inst = req.tokenEntry.instanceUrl;
  if (isNaN(aid)) return res.status(400).json({ error: 'Invalid artist ID' });
  try {
    var infoData = await hifiGetForToken(inst, '/artist/', { id: aid });
    var artistInfo = {};
    if      (infoData.artist && infoData.artist.id)                            artistInfo = infoData.artist;
    else if (infoData.data && infoData.data.artist && infoData.data.artist.id) artistInfo = infoData.data.artist;
    else if (infoData.id && infoData.name)                                     artistInfo = infoData;
    else if (infoData.data && infoData.data.id && infoData.data.name)          artistInfo = infoData.data;

    var coverData = infoData.cover || {}, albumItems = [], allTracks = [];

    try {
      var discData = await hifiGetForToken(inst, '/artist/', { f: aid, skip_tracks: false });
      if      (discData.albums && Array.isArray(discData.albums))  albumItems = discData.albums;
      else if (discData.albums && discData.albums.items)            albumItems = discData.albums.items;
      if      (discData.tracks && Array.isArray(discData.tracks))  allTracks  = discData.tracks;
      else if (discData.tracks && discData.tracks.items)            allTracks  = discData.tracks.items;
    } catch (e2) { console.log('[artist] disc call failed:', e2.message); }

    if (!albumItems.length) {
      try {
        var albData = await hifiGetForToken(inst, '/artist/albums/', { id: aid, limit: 50, offset: 0 });
        albumItems = Array.isArray(albData) ? albData : albData.items ? albData.items : (albData.data && Array.isArray(albData.data)) ? albData.data : (albData.data && albData.data.items) ? albData.data.items : [];
      } catch (e3) { console.log('[artist] /artist/albums/ failed:', e3.message); }
    }

    if (!albumItems.length && artistInfo.name) {
      try {
        var sData = await hifiGetForToken(inst, '/search/', { s: artistInfo.name, limit: 20, offset: 0 });
        var sItems = (sData && sData.data && sData.data.items) ? sData.data.items : [];
        var aMap = {};
        sItems.forEach(function(t) {
          if (!t || !t.album || !t.album.id) return;
          var tArt = trackArtist(t).toLowerCase(), want = (artistInfo.name || '').toLowerCase();
          if (tArt.indexOf(want) === -1 && want.indexOf(tArt) === -1) return;
          var alId = String(t.album.id);
          if (!aMap[alId]) aMap[alId] = { id: alId, title: t.album.title, cover: t.album.cover, releaseDate: t.album.releaseDate, numberOfTracks: t.album.numberOfTracks };
        });
        albumItems = Object.values(aMap);
      } catch (e5) { console.log('[artist] search fallback failed:', e5.message); }
    }

    var artistName = artistInfo.name || 'Unknown';
    var artworkURL = coverData['750'] || coverUrl(artistInfo.picture, 480);
    var topTracks = [];
    allTracks.filter(function(t) { return t && t.id && t.streamReady !== false && t.allowStreaming !== false; })
      .sort(function(a, b) { return (b.popularity || 0) - (a.popularity || 0); }).slice(0, 20)
      .forEach(function(t) { topTracks.push({ id: String(t.id), title: t.title || 'Unknown', artist: trackArtist(t) || artistName, duration: trackDuration(t), artworkURL: coverUrl(t.album && t.album.cover) }); });

    var albums = [];
    albumItems.filter(function(a) { return a && a.id; })
      .sort(function(a, b) { return (b.releaseDate || '').localeCompare(a.releaseDate || ''); }).slice(0, 60)
      .forEach(function(al) { albums.push({ id: String(al.id), title: al.title || 'Unknown', artist: artistName, artworkURL: coverUrl(al.cover), trackCount: al.numberOfTracks || undefined, year: (al.releaseDate || '').slice(0, 4) || undefined }); });

    console.log('[artist] returning topTracks:', topTracks.length, 'albums:', albums.length);
    res.json({ id: String(artistInfo.id || aid), name: artistName, artworkURL: artworkURL, bio: null, topTracks: topTracks, albums: albums });
  } catch (e) { console.error('[artist] ' + e.message); res.status(502).json({ error: 'Artist fetch failed: ' + e.message }); }
});

// ─── Playlist ─────────────────────────────────────────────────────────────────
app.get('/u/:token/playlist/:id', tokenMiddleware, async function(req, res) {
  var pid  = req.params.id;
  var inst = req.tokenEntry.instanceUrl;
  if (!isPlaylistUUID(pid)) {
    console.warn('[playlist] rejected non-UUID id:', pid);
    return res.status(404).json({ error: 'Invalid playlist ID. TIDAL playlist IDs must be UUIDs (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).' });
  }
  try {
    var data = await hifiGetForToken(inst, '/playlist/', { id: pid, limit: 100, offset: 0 });
    console.log('[playlist] top-level keys:', Object.keys(data));

    var pl = null, rawItems = [];
    if      (data.playlist && (data.playlist.uuid || data.playlist.id)) { pl = data.playlist; rawItems = data.items || data.playlist.items || []; }
    else if (data.data && data.data.playlist)                            { pl = data.data.playlist; rawItems = data.data.items || data.items || []; }
    else if (data.uuid || (data.title && data.numberOfTracks !== undefined)) { pl = data; rawItems = data.items || []; }
    else if (data.data && (data.data.uuid || data.data.title))           { pl = data.data; rawItems = data.data.items || data.items || []; }
    else                                                                 { pl = data; rawItems = data.items || []; }

    console.log('[playlist] title:', pl && pl.title, '| items:', rawItems.length);
    var tracks = [];
    for (var i = 0; i < rawItems.length; i++) {
      var t = rawItems[i].item || rawItems[i];
      if (!t || !t.id || t.streamReady === false) continue;
      tracks.push({ id: String(t.id), title: t.title || 'Unknown', artist: trackArtist(t), duration: trackDuration(t), artworkURL: coverUrl(t.album && t.album.cover) });
    }

    res.json({ id: String((pl && (pl.uuid || pl.id)) || pid), title: (pl && pl.title) || 'Playlist', creator: (pl && pl.creator && pl.creator.name) || undefined, artworkURL: (pl && (pl.squareImage || pl.image)) ? coverUrl(pl.squareImage || pl.image, 480) : undefined, trackCount: (pl && pl.numberOfTracks) || tracks.length, tracks: tracks });
  } catch (e) { console.error('[playlist] ' + e.message); res.status(502).json({ error: 'Playlist fetch failed: ' + e.message }); }
});

// ─── Keep-alive self-ping (Render only — skipped on Vercel automatically) ────
(function startKeepAlive() {
  var selfUrl = process.env.RENDER_EXTERNAL_URL || null;
  if (!selfUrl) {
    console.log('[keep-alive] RENDER_EXTERNAL_URL not set — skipping self-ping.');
    return;
  }
  var pingUrl = selfUrl.replace(/\/+$/, '') + '/health';
  setInterval(function () {
    axios.get(pingUrl, { timeout: 10000 })
      .then(function () { console.log('[keep-alive] ping ok -> ' + pingUrl); })
      .catch(function (e) { console.warn('[keep-alive] ping failed: ' + e.message); });
  }, 14 * 60 * 1000);
  console.log('[keep-alive] started, pinging ' + pingUrl + ' every 14 min');
})();

// ─── Start ────────────────────────────────────────────────────────────────────
if (require.main === module) {
  app.listen(PORT, function() { console.log('Claudochrome v2.0.0 (Hi-Fi API v2.7) on port ' + PORT); });
}
module.exports = app;
