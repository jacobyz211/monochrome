const express = require('express');
const cors    = require('cors');
const axios   = require('axios');
const crypto  = require('crypto');
const Redis   = require('ioredis');

const app  = express();
const PORT = process.env.PORT || 3000;
app.use(cors());
app.use(express.json());

// ─── Hi-Fi API instances (TIDAL proxy, no account needed) ─────────────────
const HIFI_INSTANCES = [
  'https://ohio.monochrome.tf',
  'https://virginia.monochrome.tf',
  'https://oregon.monochrome.tf',
  'https://frankfurt.monochrome.tf'
];
let activeInstance  = HIFI_INSTANCES[0];
let instanceHealthy = false;

const COUNTRY = process.env.TIDAL_COUNTRY || 'US';
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

function coverUrl(uuid, size) {
  if (!uuid) return null;
  size = size || 320;
  return 'https://resources.tidal.com/images/' + uuid.replace(/-/g, '/') + '/' + size + 'x' + size + '.jpg';
}

function trackDuration(t) { return t && t.duration ? Math.floor(t.duration) : null; }

function trackArtist(t) {
  if (!t) return 'Unknown';
  if (t.artists && t.artists.length) return t.artists.map(function (a) { return a.name; }).join(', ');
  if (t.artist && t.artist.name) return t.artist.name;
  return 'Unknown';
}

async function hifiGet(path, params) {
  var errors = [];
  var instances = instanceHealthy ? [activeInstance].concat(HIFI_INSTANCES.filter(function (i) { return i !== activeInstance; })) : HIFI_INSTANCES.slice();
  for (var inst of instances) {
    try {
      var r = await axios.get(inst + '/v1' + path, {
        params: Object.assign({ countryCode: COUNTRY }, params || {}),
        headers: { 'User-Agent': UA, 'Accept': 'application/json' },
        timeout: 12000
      });
      if (r.status === 200 && r.data) {
        if (inst !== activeInstance) { activeInstance = inst; instanceHealthy = true; console.log('[hifi] switched to ' + inst); }
        return r.data;
      }
    } catch (e) {
      errors.push(inst + ': ' + e.message);
      console.warn('[hifi] ' + inst + ' failed: ' + e.message.slice(0, 80));
    }
  }
  throw new Error('All Hi-Fi instances failed. Last errors: ' + errors.slice(-2).join(' | '));
}

async function checkInstances() {
  for (var inst of HIFI_INSTANCES) {
    try {
      await axios.get(inst + '/v1/search', { params: { query: 'test', types: 'TRACKS', limit: 1, countryCode: COUNTRY }, timeout: 8000 });
      activeInstance  = inst;
      instanceHealthy = true;
      console.log('[hifi] healthy instance: ' + inst);
      return;
    } catch (e) { /* try next */ }
  }
  instanceHealthy = false;
  console.warn('[hifi] WARNING: no healthy instances at startup. Will retry on first request.');
}

checkInstances();
setInterval(checkInstances, 15 * 60 * 1000);

// ─── Redis ─────────────────────────────────────────────────────────────────
let redis = null;
if (process.env.REDIS_URL) {
  redis = new Redis(process.env.REDIS_URL, { maxRetriesPerRequest: 3, enableReadyCheck: false });
  redis.on('connect', function () { console.log('[Redis] connected'); });
  redis.on('error',   function (e) { console.error('[Redis] ' + e.message); });
}

async function redisSave(token, entry) {
  if (!redis) return;
  try { await redis.set('mc:token:' + token, JSON.stringify({ createdAt: entry.createdAt, lastUsed: entry.lastUsed, reqCount: entry.reqCount })); }
  catch (e) { console.error('[Redis] save failed: ' + e.message); }
}

async function redisLoad(token) {
  if (!redis) return null;
  try { var d = await redis.get('mc:token:' + token); return d ? JSON.parse(d) : null; }
  catch (e) { return null; }
}

// ─── Token auth ─────────────────────────────────────────────────────────────
const TOKEN_CACHE       = new Map();
const IP_CREATES        = new Map();
const MAX_TOKENS_PER_IP = 10;
const RATE_MAX          = 80;
const RATE_WINDOW_MS    = 60000;

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
  var entry = { createdAt: saved.createdAt, lastUsed: saved.lastUsed, reqCount: saved.reqCount, rateWin: [] };
  TOKEN_CACHE.set(token, entry);
  return entry;
}

function checkRateLimit(entry) {
  var now = Date.now();
  entry.rateWin = (entry.rateWin || []).filter(function (t) { return now - t < RATE_WINDOW_MS; });
  if (entry.rateWin.length >= RATE_MAX) return false;
  entry.rateWin.push(now); entry.lastUsed = now; entry.reqCount = (entry.reqCount || 0) + 1;
  return true;
}

async function tokenMiddleware(req, res, next) {
  var entry = await getTokenEntry(req.params.token);
  if (!entry) return res.status(404).json({ error: 'Invalid token.' });
  if (!checkRateLimit(entry)) return res.status(429).json({ error: 'Rate limit exceeded.' });
  req.tokenEntry = entry;
  if (entry.reqCount % 20 === 0) redisSave(req.params.token, entry);
  next();
}

function getBaseUrl(req) {
  return (req.headers['x-forwarded-proto'] || req.protocol) + '://' + req.get('host');
}

// ─── Config page ─────────────────────────────────────────────────────────────
function buildConfigPage(baseUrl) {
  var h = '';
  h += '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">';
  h += '<meta name="viewport" content="width=device-width,initial-scale=1">';
  h += '<title>Claudochrome - TIDAL Addon</title>';
  h += '<style>';
  h += '*{box-sizing:border-box;margin:0;padding:0}';
  h += 'body{background:#080808;color:#e0e0e0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;min-height:100vh;display:flex;flex-direction:column;align-items:center;padding:48px 20px 64px}';
  h += '.logo{margin-bottom:22px}';
  h += '.card{background:#111;border:1px solid #1e1e1e;border-radius:18px;padding:36px;max-width:540px;width:100%;box-shadow:0 24px 64px rgba(0,0,0,.6);margin-bottom:20px}';
  h += 'h1{font-size:22px;font-weight:700;margin-bottom:6px;color:#fff}h2{font-size:16px;font-weight:700;margin-bottom:14px;color:#fff}';
  h += 'p.sub{font-size:14px;color:#666;margin-bottom:20px;line-height:1.6}';
  h += '.tip{background:#0a0a0a;border:1px solid #1e1e1e;border-radius:10px;padding:12px 14px;margin-bottom:20px;font-size:12px;color:#888;line-height:1.7}.tip b{color:#ccc}';
  h += '.pills{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:24px}';
  h += '.pill{border-radius:20px;font-size:11px;font-weight:600;padding:4px 10px;background:#181818;color:#aaa;border:1px solid #2a2a2a}';
  h += '.pill.hi{background:#0d1520;color:#4a9eff;border-color:#1a3050}';
  h += '.pill.lf{background:#1a0d0d;color:#e06060;border-color:#3a1818}';
  h += '.lbl{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:#444;margin-bottom:8px;margin-top:16px}';
  h += 'input{width:100%;background:#0a0a0a;border:1px solid #1e1e1e;border-radius:10px;color:#e0e0e0;font-size:14px;padding:12px 14px;margin-bottom:6px;outline:none;transition:border-color .15s}';
  h += 'input:focus{border-color:#fff}input::placeholder{color:#2e2e2e}';
  h += '.hint{font-size:12px;color:#3a3a3a;margin-bottom:12px;line-height:1.7}.hint code{background:#1a1a1a;padding:1px 5px;border-radius:4px;color:#777}';
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
  h += '.status{font-size:13px;color:#555;margin:8px 0;min-height:18px}.status.ok{color:#4a9a4a}.status.err{color:#c04040}';
  h += '.preview{background:#0a0a0a;border:1px solid #161616;border-radius:10px;padding:12px;max-height:200px;overflow-y:auto;margin-bottom:12px;display:none}';
  h += '.tr{display:flex;gap:10px;align-items:center;padding:5px 0;border-bottom:1px solid #141414;font-size:13px}.tr:last-child{border-bottom:none}';
  h += '.tn{color:#333;font-size:11px;min-width:22px;text-align:right}.ti{flex:1;min-width:0}';
  h += '.tt{color:#e0e0e0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.ta{color:#555;font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}';
  h += '.inst-list{display:flex;flex-direction:column;gap:6px;margin-top:10px}';
  h += '.inst{display:flex;align-items:center;gap:8px;font-size:12px;padding:8px 12px;background:#0a0a0a;border:1px solid #161616;border-radius:8px}';
  h += '.dot{width:7px;height:7px;border-radius:50%;background:#333;flex-shrink:0}.dot.ok{background:#4a9a4a}.dot.err{background:#c04040}';
  h += '.inst-url{flex:1;color:#666;font-family:"SF Mono","Fira Code",monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}';
  h += '.inst-ms{color:#444;margin-left:auto;font-size:11px}';
  h += 'footer{margin-top:32px;font-size:12px;color:#2a2a2a;text-align:center;line-height:1.8}';
  h += '</style></head><body>';

  h += '<svg class="logo" width="52" height="52" viewBox="0 0 52 52" fill="none">';
  h += '<circle cx="26" cy="26" r="26" fill="#fff"/>';
  h += '<rect x="10" y="20" width="4" height="12" rx="2" fill="#000"/>';
  h += '<rect x="17" y="14" width="4" height="24" rx="2" fill="#000"/>';
  h += '<rect x="24" y="18" width="4" height="16" rx="2" fill="#000"/>';
  h += '<rect x="31" y="11" width="4" height="30" rx="2" fill="#000"/>';
  h += '<rect x="38" y="17" width="4" height="18" rx="2" fill="#000"/>';
  h += '</svg>';

  h += '<div class="card">';
  h += '<h1>Claudochrome for Eclipse</h1>';
  h += '<p class="sub">Full TIDAL catalog — lossless FLAC, HiRes, AAC 320 — no account, no subscription. Powered by the Hi-Fi API.</p>';
  h += '<div class="tip"><b>Save your URL.</b> Paste it below to refresh if the server restarts, without breaking any playlists.</div>';
  h += '<div class="pills"><span class="pill">Tracks · Albums · Artists</span><span class="pill hi">FLAC / HiRes</span><span class="pill hi">AAC 320</span><span class="pill lf">Last.fm ready</span><span class="pill">CSV import</span></div>';
  h += '<button class="bw" id="genBtn" onclick="generate()">Generate My Addon URL</button>';
  h += '<div class="box" id="genBox"><div class="blbl">Your addon URL — paste into Eclipse</div><div class="burl" id="genUrl"></div><button class="bd" id="copyGenBtn" onclick="copyGen()">Copy URL</button></div>';
  h += '<hr><div class="lbl">Refresh existing URL</div>';
  h += '<input type="text" id="existingUrl" placeholder="Paste your existing addon URL here">';
  h += '<div class="hint">Keeps the same URL active in Eclipse — nothing to reinstall.</div>';
  h += '<button class="bg" id="refBtn" onclick="doRefresh()">Refresh Existing URL</button>';
  h += '<div class="box" id="refBox"><div class="blbl">Refreshed — same URL, still works in Eclipse</div><div class="burl" id="refUrl"></div><button class="bd" id="copyRefBtn" onclick="copyRef()">Copy URL</button></div>';
  h += '<hr><div class="steps">';
  h += '<div class="step"><div class="sn">1</div><div class="st">Generate and copy your URL above</div></div>';
  h += '<div class="step"><div class="sn">2</div><div class="st">Open <b>Eclipse</b> → Settings → Connections → Add Connection → Addon</div></div>';
  h += '<div class="step"><div class="sn">3</div><div class="st">Paste your URL and tap Install</div></div>';
  h += '<div class="step"><div class="sn">4</div><div class="st">Search TIDAL\'s full catalog — FLAC quality auto-selected</div></div>';
  h += '</div>';
  h += '<div class="warn">Hi-Fi instances are community-hosted and proxy TIDAL without a subscription. If all instances are offline, streams will fail until one recovers. The addon checks instance health every 15 minutes.</div>';
  h += '</div>';

  h += '<div class="card">';
  h += '<h2>Instance Health</h2>';
  h += '<p class="sub" style="margin-bottom:14px">Live status of all Hi-Fi API instances used for failover.</p>';
  h += '<div class="inst-list" id="instList"><div style="color:#333;font-size:13px">Checking...</div></div>';
  h += '<button class="bg" style="margin-top:14px" onclick="checkHealth()">Refresh Status</button>';
  h += '</div>';

  h += '<div class="card">';
  h += '<h2>Playlist Importer</h2>';
  h += '<p class="sub">Imports a TIDAL album or playlist as a CSV for Eclipse Library → Import → CSV.</p>';
  h += '<div class="lbl">Your Addon URL</div>';
  h += '<input type="text" id="impToken" placeholder="Auto-fills after generating above">';
  h += '<div class="lbl">TIDAL Album or Playlist ID</div>';
  h += '<input type="text" id="impId" placeholder="TIDAL album ID or playlist UUID">';
  h += '<div class="hint">Album ID: the number in a TIDAL album URL. Playlist UUID: the long string in a TIDAL playlist URL.</div>';
  h += '<div style="display:flex;gap:8px;margin-top:4px">';
  h += '<button class="bg" id="impAlbBtn" style="margin:0" onclick="doImport(\'album\')">Import Album</button>';
  h += '<button class="bg" id="impPlBtn"  style="margin:0" onclick="doImport(\'playlist\')">Import Playlist</button>';
  h += '</div>';
  h += '<div class="status" id="impStatus" style="margin-top:10px"></div>';
  h += '<div class="preview" id="impPreview"></div>';
  h += '</div>';

  h += '<footer>Claudochrome Eclipse Addon v1.0.0 &bull; Hi-Fi API &bull; <a href="' + baseUrl + '/health" target="_blank" style="color:#2a2a2a;text-decoration:none">' + baseUrl + '/health</a></footer>';

  h += '<script>';
  h += 'var _gu="",_ru="";';

  h += 'function generate(){';
  h += 'var btn=document.getElementById("genBtn");btn.disabled=true;btn.textContent="Generating...";';
  h += 'fetch("/generate",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({})})';
  h += '.then(function(r){return r.json();}).then(function(d){';
  h += 'if(d.error){alert(d.error);btn.disabled=false;btn.textContent="Generate My Addon URL";return;}';
  h += '_gu=d.manifestUrl;document.getElementById("genUrl").textContent=_gu;document.getElementById("genBox").style.display="block";';
  h += 'document.getElementById("impToken").value=_gu;';
  h += 'btn.disabled=false;btn.textContent="Regenerate URL";';
  h += '}).catch(function(e){alert("Error: "+e.message);btn.disabled=false;btn.textContent="Generate My Addon URL";});}';

  h += 'function copyGen(){if(!_gu)return;navigator.clipboard.writeText(_gu).then(function(){var b=document.getElementById("copyGenBtn");b.textContent="Copied!";setTimeout(function(){b.textContent="Copy URL";},1500);});}';

  h += 'function doRefresh(){';
  h += 'var btn=document.getElementById("refBtn"),eu=document.getElementById("existingUrl").value.trim();';
  h += 'if(!eu){alert("Paste your existing addon URL first.");return;}';
  h += 'btn.disabled=true;btn.textContent="Refreshing...";';
  h += 'fetch("/refresh",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({existingUrl:eu})})';
  h += '.then(function(r){return r.json();}).then(function(d){';
  h += 'if(d.error){alert(d.error);btn.disabled=false;btn.textContent="Refresh Existing URL";return;}';
  h += '_ru=d.manifestUrl;document.getElementById("refUrl").textContent=_ru;document.getElementById("refBox").style.display="block";';
  h += 'document.getElementById("impToken").value=_ru;';
  h += 'btn.disabled=false;btn.textContent="Refresh Again";';
  h += '}).catch(function(e){alert("Error: "+e.message);btn.disabled=false;btn.textContent="Refresh Existing URL";});}';

  h += 'function copyRef(){if(!_ru)return;navigator.clipboard.writeText(_ru).then(function(){var b=document.getElementById("copyRefBtn");b.textContent="Copied!";setTimeout(function(){b.textContent="Copy URL";},1500);});}';

  h += 'function checkHealth(){';
  h += 'var list=document.getElementById("instList");list.innerHTML=\'<div style="color:#333;font-size:13px">Checking...</div>\';';
  h += 'fetch("/instances").then(function(r){return r.json();}).then(function(data){';
  h += 'list.innerHTML="";';
  h += '(data.instances||[]).forEach(function(inst){';
  h += 'var d=document.createElement("div");d.className="inst";';
  h += 'var dot=\'<span class="dot\'+(inst.ok?" ok":" err")+'"\></span>\';';
  h += 'var ms=inst.ok?(\'<span class="inst-ms">\'+inst.ms+\'ms</span>\'):\'\';';
  h += 'd.innerHTML=dot+\'<span class="inst-url">\'+inst.url+\'</span>\'+ms;list.appendChild(d);';
  h += '});';
  h += '}).catch(function(){list.innerHTML=\'<div style="color:#c04040;font-size:13px">Could not reach server</div>\';});}';
  h += 'checkHealth();';

  h += 'function getTok(s){var m=s.match(/\\/u\\/([a-f0-9]{28})\\//);return m?m[1]:null;}';
  h += 'function hesc(s){return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}';

  h += 'function doImport(type){';
  h += 'var raw=document.getElementById("impToken").value.trim(),id=document.getElementById("impId").value.trim(),st=document.getElementById("impStatus"),pv=document.getElementById("impPreview");';
  h += 'if(!raw){st.className="status err";st.textContent="Paste your addon URL first.";return;}';
  h += 'if(!id){st.className="status err";st.textContent="Enter an album or playlist ID.";return;}';
  h += 'var tok=getTok(raw);if(!tok){st.className="status err";st.textContent="Could not find your token.";return;}';
  h += 'var btns=[document.getElementById("impAlbBtn"),document.getElementById("impPlBtn")];';
  h += 'btns.forEach(function(b){b.disabled=true;});';
  h += 'st.className="status";st.textContent="Fetching...";pv.style.display="none";';
  h += 'fetch("/u/"+tok+"/import-csv?type="+type+"&id="+encodeURIComponent(id))';
  h += '.then(function(r){if(!r.ok)return r.json().then(function(e){throw new Error(e.error||"Server error "+r.status);});return r.json();})';
  h += '.then(function(data){';
  h += 'var tracks=data.tracks||[];if(!tracks.length)throw new Error("No tracks found.");';
  h += 'var rows=tracks.slice(0,50).map(function(t,i){return\'<div class="tr"><span class="tn">\'+(i+1)+\'</span><div class="ti"><div class="tt">\'+hesc(t.title)+\'</div><div class="ta">\'+hesc(t.artist)+\'</div></div></div>\';}).join("");';
  h += 'if(tracks.length>50)rows+=\'<div class="tr" style="text-align:center;color:#333">+\'+(tracks.length-50)+\' more</div>\';';
  h += 'pv.innerHTML=rows;pv.style.display="block";';
  h += 'st.className="status ok";st.textContent="Found "+tracks.length+" tracks in \\""+hesc(data.title)+"\\"";';
  h += 'var lines=["Title,Artist,Album,Duration"];';
  h += 'tracks.forEach(function(t){function ce(s){s=String(s||"");if(s.indexOf(",")!==-1||s.indexOf("\\"")!==-1){s=\'"\'+s.replace(/"/g,\'""\')+\'"\'}return s;}lines.push(ce(t.title)+","+ce(t.artist)+","+ce(data.title||"")+","+ce(t.duration||""));});';
  h += 'var blob=new Blob([lines.join("\\n")],{type:"text/csv"});var a=document.createElement("a");a.href=URL.createObjectURL(blob);a.download=(data.title||"playlist").replace(/[^a-zA-Z0-9 _-]/g,"").trim()+".csv";document.body.appendChild(a);a.click();document.body.removeChild(a);';
  h += 'btns.forEach(function(b){b.disabled=false;});';
  h += '}).catch(function(e){st.className="status err";st.textContent=e.message;btns.forEach(function(b){b.disabled=false;});});}';

  h += '<\/script></body></html>';
  return h;
}

// ─── Routes ────────────────────────────────────────────────────────────────
app.get('/', function (req, res) {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(buildConfigPage(getBaseUrl(req)));
});

app.post('/generate', async function (req, res) {
  var ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown').split(',')[0].trim();
  var bucket = getOrCreateIpBucket(ip);
  if (bucket.count >= MAX_TOKENS_PER_IP) return res.status(429).json({ error: 'Too many tokens from this IP today.' });
  var token = generateToken();
  var entry = { createdAt: Date.now(), lastUsed: Date.now(), reqCount: 0, rateWin: [] };
  TOKEN_CACHE.set(token, entry);
  await redisSave(token, entry);
  bucket.count++;
  res.json({ token: token, manifestUrl: getBaseUrl(req) + '/u/' + token + '/manifest.json' });
});

app.post('/refresh', async function (req, res) {
  var raw = (req.body && req.body.existingUrl) ? String(req.body.existingUrl).trim() : '';
  var token = raw, m = raw.match(/\/u\/([a-f0-9]{28})\//);
  if (m) token = m[1];
  if (!token || !/^[a-f0-9]{28}$/.test(token)) return res.status(400).json({ error: 'Paste your full addon URL.' });
  var entry = await getTokenEntry(token);
  if (!entry) return res.status(404).json({ error: 'URL not found. Generate a new one.' });
  res.json({ token: token, manifestUrl: getBaseUrl(req) + '/u/' + token + '/manifest.json', refreshed: true });
});

app.get('/instances', async function (_req, res) {
  var results = await Promise.all(HIFI_INSTANCES.map(async function (inst) {
    var start = Date.now();
    try {
      await axios.get(inst + '/v1/search', { params: { query: 'test', types: 'TRACKS', limit: 1, countryCode: COUNTRY }, timeout: 6000 });
      return { url: inst, ok: true, ms: Date.now() - start };
    } catch (e) { return { url: inst, ok: false, ms: null }; }
  }));
  res.json({ instances: results });
});

app.get('/u/:token/manifest.json', tokenMiddleware, function (req, res) {
  res.json({
    id:          'com.eclipse.claudochrome.' + req.params.token.slice(0, 8),
    name:        'Claudochrome (TIDAL)',
    version:     '1.0.0',
    description: 'Full TIDAL catalog via Hi-Fi API. Lossless FLAC, AAC 320. No account required.',
    icon:        'https://monochrome.tf/favicon.ico',
    resources:   ['search', 'stream', 'catalog'],
    types:       ['track', 'album', 'artist', 'playlist']
  });
});

app.get('/u/:token/search', tokenMiddleware, async function (req, res) {
  var q = String(req.query.q || '').trim();
  if (!q) return res.json({ tracks: [], albums: [], artists: [], playlists: [] });
  try {
    var data = await hifiGet('/search', { query: q, types: 'TRACKS,ALBUMS,ARTISTS', limit: 20, offset: 0 });
    var tracks  = (data.tracks  && data.tracks.items)  || [];
    var albums  = (data.albums  && data.albums.items)  || [];
    var artists = (data.artists && data.artists.items) || [];
    res.json({
      tracks: tracks.filter(function (t) { return t.streamReady !== false && t.allowStreaming !== false; }).map(function (t) {
        return {
          id:         String(t.id),
          title:      t.title || 'Unknown',
          artist:     trackArtist(t),
          album:      (t.album && t.album.title) || null,
          duration:   trackDuration(t),
          artworkURL: coverUrl(t.album && t.album.cover),
          format:     'flac'
        };
      }),
      albums: albums.map(function (a) {
        return {
          id:         String(a.id),
          title:      a.title || 'Unknown',
          artist:     (a.artist && a.artist.name) || 'Unknown',
          artworkURL: coverUrl(a.cover),
          trackCount: a.numberOfTracks || null,
          year:       (a.releaseDate || '').slice(0, 4) || null
        };
      }),
      artists: artists.map(function (a) {
        return {
          id:         String(a.id),
          name:       a.name || 'Unknown',
          artworkURL: (a.picture ? coverUrl(a.picture) : null)
        };
      }),
      playlists: []
    });
  } catch (e) {
    console.error('[search] ' + e.message);
    res.status(502).json({ error: 'Search failed: ' + e.message, tracks: [] });
  }
});

app.get('/u/:token/stream/:id', tokenMiddleware, async function (req, res) {
  var tid = req.params.id;
  var qualities = ['LOSSLESS', 'HIGH', 'LOW'];
  for (var q of qualities) {
    try {
      var data = await hifiGet('/tracks/' + tid + '/streamUrl', { soundQuality: q });
      if (data && data.url) {
        var fmt = (data.codec && data.codec.toLowerCase().indexOf('flac') !== -1) ? 'flac' : 'aac';
        return res.json({
          url:       data.url,
          format:    fmt,
          quality:   q === 'LOSSLESS' ? 'lossless' : (q === 'HIGH' ? '320kbps' : '128kbps'),
          expiresAt: Math.floor(Date.now() / 1000) + 21600
        });
      }
    } catch (e) {
      if (q === 'LOW') {
        console.error('[stream] all qualities failed for ' + tid + ': ' + e.message);
        return res.status(502).json({ error: 'Could not get stream URL for track ' + tid });
      }
    }
  }
  return res.status(404).json({ error: 'No stream found for track ' + tid });
});

app.get('/u/:token/album/:id', tokenMiddleware, async function (req, res) {
  var aid = req.params.id;
  try {
    var results = await Promise.all([
      hifiGet('/albums/' + aid),
      hifiGet('/albums/' + aid + '/tracks', { limit: 100, offset: 0 })
    ]);
    var album  = results[0];
    var tracks = (results[1] && results[1].items) || [];
    res.json({
      id:         String(album.id),
      title:      album.title || 'Unknown',
      artist:     (album.artist && album.artist.name) || 'Unknown',
      artworkURL: coverUrl(album.cover, 640),
      year:       (album.releaseDate || '').slice(0, 4) || null,
      trackCount: album.numberOfTracks || tracks.length,
      tracks:     tracks.filter(function (t) { return t.streamReady !== false; }).map(function (t) {
        return {
          id:         String(t.id),
          title:      t.title || 'Unknown',
          artist:     trackArtist(t),
          duration:   trackDuration(t),
          artworkURL: coverUrl(album.cover)
        };
      })
    });
  } catch (e) {
    console.error('[album] ' + e.message);
    res.status(502).json({ error: 'Album fetch failed: ' + e.message });
  }
});

app.get('/u/:token/artist/:id', tokenMiddleware, async function (req, res) {
  var aid = req.params.id;
  try {
    var results = await Promise.all([
      hifiGet('/artists/' + aid),
      hifiGet('/artists/' + aid + '/toptracks', { limit: 10 }),
      hifiGet('/artists/' + aid + '/albums',    { limit: 20, filter: 'ALBUMS' })
    ]);
    var artist    = results[0];
    var topTracks = (results[1] && results[1].items) || [];
    var albums    = (results[2] && results[2].items) || [];
    res.json({
      id:         String(artist.id),
      name:       artist.name || 'Unknown',
      artworkURL: artist.picture ? coverUrl(artist.picture, 480) : null,
      bio:        null,
      topTracks:  topTracks.filter(function (t) { return t.streamReady !== false; }).map(function (t) {
        return {
          id:         String(t.id),
          title:      t.title || 'Unknown',
          artist:     trackArtist(t),
          duration:   trackDuration(t),
          artworkURL: coverUrl(t.album && t.album.cover)
        };
      }),
      albums: albums.map(function (a) {
        return {
          id:         String(a.id),
          title:      a.title || 'Unknown',
          artworkURL: coverUrl(a.cover),
          trackCount: a.numberOfTracks || null,
          year:       (a.releaseDate || '').slice(0, 4) || null
        };
      })
    });
  } catch (e) {
    console.error('[artist] ' + e.message);
    res.status(502).json({ error: 'Artist fetch failed: ' + e.message });
  }
});

app.get('/u/:token/playlist/:id', tokenMiddleware, async function (req, res) {
  var pid = req.params.id;
  try {
    var results = await Promise.all([
      hifiGet('/playlists/' + pid),
      hifiGet('/playlists/' + pid + '/tracks', { limit: 100, offset: 0 })
    ]);
    var pl     = results[0];
    var tracks = (results[1] && results[1].items) || [];
    res.json({
      id:         String(pl.uuid || pl.id),
      title:      pl.title || 'Playlist',
      creator:    (pl.creator && pl.creator.name) || null,
      artworkURL: pl.squareImage ? coverUrl(pl.squareImage) : null,
      tracks:     tracks.filter(function (t) { return t.streamReady !== false; }).map(function (t) {
        return {
          id:         String(t.id),
          title:      t.title || 'Unknown',
          artist:     trackArtist(t),
          duration:   trackDuration(t),
          artworkURL: coverUrl(t.album && t.album.cover)
        };
      })
    });
  } catch (e) {
    console.error('[playlist] ' + e.message);
    res.status(502).json({ error: 'Playlist fetch failed: ' + e.message });
  }
});

app.get('/u/:token/import-csv', tokenMiddleware, async function (req, res) {
  var type = req.query.type, id = String(req.query.id || '').trim();
  if (!type || !id) return res.status(400).json({ error: 'Pass ?type=album|playlist&id=...' });
  try {
    if (type === 'album') {
      var results = await Promise.all([
        hifiGet('/albums/' + id),
        hifiGet('/albums/' + id + '/tracks', { limit: 100 })
      ]);
      var alb = results[0], items = (results[1] && results[1].items) || [];
      return res.json({
        title:  alb.title || 'Album',
        tracks: items.filter(function (t) { return t.streamReady !== false; }).map(function (t) {
          return { title: t.title || 'Unknown', artist: trackArtist(t), duration: trackDuration(t) };
        })
      });
    }
    if (type === 'playlist') {
      var results2 = await Promise.all([
        hifiGet('/playlists/' + id),
        hifiGet('/playlists/' + id + '/tracks', { limit: 100 })
      ]);
      var pl2 = results2[0], items2 = (results2[1] && results2[1].items) || [];
      return res.json({
        title:  pl2.title || 'Playlist',
        tracks: items2.filter(function (t) { return t.streamReady !== false; }).map(function (t) {
          return { title: t.title || 'Unknown', artist: trackArtist(t), duration: trackDuration(t) };
        })
      });
    }
    res.status(400).json({ error: 'type must be album or playlist.' });
  } catch (e) {
    res.status(502).json({ error: 'Import failed: ' + e.message });
  }
});

app.get('/health', function (_req, res) {
  res.json({
    status:          'ok',
    version:         '1.0.0',
    activeInstance:  activeInstance,
    instanceHealthy: instanceHealthy,
    activeTokens:    TOKEN_CACHE.size,
    redisConnected:  !!(redis && redis.status === 'ready'),
    country:         COUNTRY,
    timestamp:       new Date().toISOString()
  });
});

app.listen(PORT, function () {
  console.log('Claudochrome Eclipse Addon v1.0.0 on port ' + PORT);
});
