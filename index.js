const express = require('express');
const cors    = require('cors');
const axios   = require('axios');
const crypto  = require('crypto');
const Redis   = require('ioredis');

const app  = express();
const PORT = process.env.PORT || 3000;
app.use(cors());
app.use(express.json());

// ─── Hi-Fi API v2.7 instances ─────────────────────────────────────────────
const HIFI_INSTANCES = [
  'https://ohio-1.monochrome.tf',
  'https://frankfurt-1.monochrome.tf',
  'https://eu-central.monochrome.tf',
  'https://us-west.monochrome.tf',
  'https://hifi.geeked.wtf',
  'https://hifi-one.spotisaver.net',
  'https://monochrome-api.samidy.com'
];
let activeInstance  = HIFI_INSTANCES[0];
let instanceHealthy = false;

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36';

// ─── Helpers ─────────────────────────────────────────────────────────────────
function coverUrl(uuid, size) {
  if (!uuid) return null;
  if (String(uuid).startsWith('http')) return uuid;
  size = size || 320;
  return 'https://resources.tidal.com/images/' + uuid.replace(/-/g, '/') + '/' + size + 'x' + size + '.jpg';
}

function trackDuration(t) {
  return t && t.duration ? Math.floor(t.duration) : null;
}

function trackArtist(t) {
  if (!t) return 'Unknown';
  if (t.artists && t.artists.length) return t.artists.map(function(a) { return a.name; }).join(', ');
  if (t.artist  && t.artist.name)   return t.artist.name;
  return 'Unknown';
}

// Decode base64 manifest returned by /track/ to get actual CDN URL
function decodeManifest(manifest) {
  try {
    var decoded = JSON.parse(Buffer.from(manifest, 'base64').toString('utf8'));
    return {
      url:   (decoded.urls && decoded.urls[0]) || null,
      codec: decoded.codecs || decoded.mimeType || ''
    };
  } catch (e) { return null; }
}

// ─── Hi-Fi API client ────────────────────────────────────────────────────────
async function hifiGet(path, params) {
  var errors   = [];
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
        if (inst !== activeInstance) {
          activeInstance  = inst;
          instanceHealthy = true;
          console.log('[hifi] switched to ' + inst);
        }
        return r.data;
      }
    } catch (e) {
      errors.push(inst + ': ' + e.message);
    }
  }
  throw new Error('All Hi-Fi instances failed: ' + errors.slice(-2).join(' | '));
}

async function checkInstances() {
  for (var inst of HIFI_INSTANCES) {
    try {
      await axios.get(inst + '/search/', { params: { s: 'test', limit: 1 }, timeout: 8000 });
      activeInstance  = inst;
      instanceHealthy = true;
      console.log('[hifi] healthy: ' + inst);
      return;
    } catch (e) { /* try next */ }
  }
  instanceHealthy = false;
  console.warn('[hifi] WARNING: no healthy instances at startup.');
}

checkInstances();
setInterval(checkInstances, 15 * 60 * 1000);

// ─── Redis ───────────────────────────────────────────────────────────────────
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
      createdAt: entry.createdAt,
      lastUsed:  entry.lastUsed,
      reqCount:  entry.reqCount
    }));
  } catch (e) { /* non-fatal */ }
}

async function redisLoad(token) {
  if (!redis) return null;
  try {
    var d = await redis.get('mc:token:' + token);
    return d ? JSON.parse(d) : null;
  } catch (e) { return null; }
}

// ─── Token auth ──────────────────────────────────────────────────────────────
const TOKEN_CACHE       = new Map();
const IP_CREATES        = new Map();
const MAX_TOKENS_PER_IP = 10;
const RATE_MAX          = 80;
const RATE_WINDOW_MS    = 60000;

function generateToken() { return crypto.randomBytes(14).toString('hex'); }

function getOrCreateIpBucket(ip) {
  var now = Date.now();
  var b   = IP_CREATES.get(ip);
  if (!b || now > b.resetAt) {
    b = { count: 0, resetAt: now + 86400000 };
    IP_CREATES.set(ip, b);
  }
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
  entry.rateWin = (entry.rateWin || []).filter(function(t) { return now - t < RATE_WINDOW_MS; });
  if (entry.rateWin.length >= RATE_MAX) return false;
  entry.rateWin.push(now);
  entry.lastUsed = now;
  entry.reqCount = (entry.reqCount || 0) + 1;
  return true;
}

async function tokenMiddleware(req, res, next) {
  var entry = await getTokenEntry(req.params.token);
  if (!entry)                return res.status(404).json({ error: 'Invalid token.' });
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
  h += '.card{background:#111;border:1px solid #1e1e1e;border-radius:18px;padding:36px;max-width:540px;width:100%;box-shadow:0 24px 64px rgba(0,0,0,.6);margin-bottom:20px}';
  h += 'h1{font-size:22px;font-weight:700;margin-bottom:6px;color:#fff}';
  h += 'h2{font-size:16px;font-weight:700;margin-bottom:14px;color:#fff}';
  h += 'p.sub{font-size:14px;color:#666;margin-bottom:20px;line-height:1.6}';
  h += '.tip{background:#0a0a0a;border:1px solid #1e1e1e;border-radius:10px;padding:12px 14px;margin-bottom:20px;font-size:12px;color:#888;line-height:1.7}';
  h += '.tip b{color:#ccc}';
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
  h += '.steps{display:flex;flex-direction:column;gap:12px}';
  h += '.step{display:flex;gap:12px;align-items:flex-start}';
  h += '.sn{background:#161616;border:1px solid #222;border-radius:50%;width:26px;height:26px;min-width:26px;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:#555}';
  h += '.st{font-size:13px;color:#555;line-height:1.6}.st b{color:#999}';
  h += '.warn{background:#0d0d0d;border:1px solid #1e1e1e;border-radius:10px;padding:14px;margin-top:20px;font-size:12px;color:#555;line-height:1.7}';
  h += '.status{font-size:13px;color:#555;margin:8px 0;min-height:18px}.status.ok{color:#4a9a4a}.status.err{color:#c04040}';
  h += '.preview{background:#0a0a0a;border:1px solid #161616;border-radius:10px;padding:12px;max-height:200px;overflow-y:auto;margin-bottom:12px;display:none}';
  h += '.tr{display:flex;gap:10px;align-items:center;padding:5px 0;border-bottom:1px solid #141414;font-size:13px}';
  h += '.tr:last-child{border-bottom:none}';
  h += '.tn{color:#333;font-size:11px;min-width:22px;text-align:right}.ti{flex:1;min-width:0}';
  h += '.tt{color:#e0e0e0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}';
  h += '.ta{color:#555;font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}';
  h += '.inst-list{display:flex;flex-direction:column;gap:6px;margin-top:10px}';
  h += '.inst{display:flex;align-items:center;gap:8px;font-size:12px;padding:8px 12px;background:#0a0a0a;border:1px solid #161616;border-radius:8px}';
  h += '.dot{width:7px;height:7px;border-radius:50%;background:#333;flex-shrink:0}';
  h += '.dot.ok{background:#4a9a4a}.dot.err{background:#c04040}';
  h += '.inst-url{flex:1;color:#666;font-family:"SF Mono","Fira Code",monospace;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}';
  h += '.inst-ms{color:#444;margin-left:auto;font-size:11px}';
  h += 'footer{margin-top:32px;font-size:12px;color:#2a2a2a;text-align:center;line-height:1.8}';
  h += '</style></head><body>';

  h += '<svg width="52" height="52" viewBox="0 0 52 52" fill="none" style="margin-bottom:22px">';
  h += '<circle cx="26" cy="26" r="26" fill="#fff"/>';
  h += '<rect x="10" y="20" width="4" height="12" rx="2" fill="#000"/>';
  h += '<rect x="17" y="14" width="4" height="24" rx="2" fill="#000"/>';
  h += '<rect x="24" y="18" width="4" height="16" rx="2" fill="#000"/>';
  h += '<rect x="31" y="11" width="4" height="30" rx="2" fill="#000"/>';
  h += '<rect x="38" y="17" width="4" height="18" rx="2" fill="#000"/>';
  h += '</svg>';

  h += '<div class="card">';
  h += '<h1>Claudochrome for Eclipse</h1>';
  h += '<p class="sub">Full TIDAL catalog — lossless FLAC, HiRes, AAC 320 — no account, no subscription.</p>';
  h += '<div class="tip"><b>Save your URL.</b> Paste it below to refresh if the server restarts, without breaking playlists.</div>';
  h += '<div class="pills"><span class="pill">Tracks &middot; Albums &middot; Artists &middot; Playlists</span><span class="pill hi">FLAC / HiRes</span><span class="pill hi">AAC 320</span><span class="pill">CSV import</span></div>';
  h += '<button class="bw" id="genBtn" onclick="generate()">Generate My Addon URL</button>';
  h += '<div class="box" id="genBox"><div class="blbl">Your addon URL &mdash; paste into Eclipse</div><div class="burl" id="genUrl"></div><button class="bd" id="copyGenBtn" onclick="copyGen()">Copy URL</button></div>';
  h += '<hr>';
  h += '<div class="lbl">Refresh existing URL</div>';
  h += '<input type="text" id="existingUrl" placeholder="Paste your existing addon URL here">';
  h += '<div class="hint">Keeps the same URL active in Eclipse &mdash; nothing to reinstall.</div>';
  h += '<button class="bg" id="refBtn" onclick="doRefresh()">Refresh Existing URL</button>';
  h += '<div class="box" id="refBox"><div class="blbl">Refreshed &mdash; same URL, still works in Eclipse</div><div class="burl" id="refUrl"></div><button class="bd" id="copyRefBtn" onclick="copyRef()">Copy URL</button></div>';
  h += '<hr>';
  h += '<div class="steps">';
  h += '<div class="step"><div class="sn">1</div><div class="st">Generate and copy your URL above</div></div>';
  h += '<div class="step"><div class="sn">2</div><div class="st">Open <b>Eclipse</b> &rarr; Settings &rarr; Connections &rarr; Add Connection &rarr; Addon</div></div>';
  h += '<div class="step"><div class="sn">3</div><div class="st">Paste your URL and tap Install</div></div>';
  h += '<div class="step"><div class="sn">4</div><div class="st">Search TIDAL\'s full catalog &mdash; FLAC quality auto-selected</div></div>';
  h += '</div>';
  h += '<div class="warn">Hi-Fi instances are community-hosted. The addon auto-discovers working instances and fails over automatically.</div>';
  h += '</div>';

  h += '<div class="card">';
  h += '<h2>Instance Health</h2>';
  h += '<p class="sub" style="margin-bottom:14px">Live status of all Hi-Fi API v2.7 instances.</p>';
  h += '<div class="inst-list" id="instList"><div style="color:#333;font-size:13px">Checking...</div></div>';
  h += '<button class="bg" style="margin-top:14px" onclick="checkHealth()">Refresh Status</button>';
  h += '</div>';

  h += '<div class="card">';
  h += '<h2>Playlist / Album Importer</h2>';
  h += '<p class="sub">Import a TIDAL album or playlist as a CSV for Eclipse Library &rarr; Import &rarr; CSV.</p>';
  h += '<div class="lbl">Your Addon URL</div>';
  h += '<input type="text" id="impToken" placeholder="Auto-fills after generating above">';
  h += '<div class="lbl">TIDAL Album ID or Playlist UUID</div>';
  h += '<input type="text" id="impId" placeholder="Album: numeric ID &bull; Playlist: UUID string">';
  h += '<div class="hint">Album ID: number in the TIDAL album URL. Playlist UUID: long string in the TIDAL playlist URL.</div>';
  h += '<div style="display:flex;gap:8px;margin-top:4px">';
  h += '<button class="bg" id="impAlbBtn" style="margin:0" onclick="doImport(\'album\')">Import Album</button>';
  h += '<button class="bg" id="impPlBtn" style="margin:0" onclick="doImport(\'playlist\')">Import Playlist</button>';
  h += '</div>';
  h += '<div class="status" id="impStatus" style="margin-top:10px"></div>';
  h += '<div class="preview" id="impPreview"></div>';
  h += '</div>';

  h += '<footer>Claudochrome Eclipse Addon v1.3.0 &bull; Hi-Fi API v2.7</footer>';

  h += '<script>';
  h += 'var _gu="",_ru="";';

  h += 'function generate(){';
  h += '  var btn=document.getElementById("genBtn");';
  h += '  btn.disabled=true;btn.textContent="Generating...";';
  h += '  fetch("/generate",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"})';
  h += '  .then(function(r){return r.json();})';
  h += '  .then(function(d){';
  h += '    if(d.error){alert(d.error);btn.disabled=false;btn.textContent="Generate My Addon URL";return;}';
  h += '    _gu=d.manifestUrl;';
  h += '    document.getElementById("genUrl").textContent=_gu;';
  h += '    document.getElementById("genBox").style.display="block";';
  h += '    document.getElementById("impToken").value=_gu;';
  h += '    btn.disabled=false;btn.textContent="Regenerate URL";';
  h += '  }).catch(function(e){alert("Error: "+e.message);btn.disabled=false;btn.textContent="Generate My Addon URL";});';
  h += '}';

  h += 'function copyGen(){';
  h += '  if(!_gu)return;';
  h += '  navigator.clipboard.writeText(_gu).then(function(){var b=document.getElementById("copyGenBtn");b.textContent="Copied!";setTimeout(function(){b.textContent="Copy URL";},1500);});';
  h += '}';

  h += 'function doRefresh(){';
  h += '  var btn=document.getElementById("refBtn");';
  h += '  var eu=document.getElementById("existingUrl").value.trim();';
  h += '  if(!eu){alert("Paste your existing addon URL first.");return;}';
  h += '  btn.disabled=true;btn.textContent="Refreshing...";';
  h += '  fetch("/refresh",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({existingUrl:eu})})';
  h += '  .then(function(r){return r.json();})';
  h += '  .then(function(d){';
  h += '    if(d.error){alert(d.error);btn.disabled=false;btn.textContent="Refresh Existing URL";return;}';
  h += '    _ru=d.manifestUrl;';
  h += '    document.getElementById("refUrl").textContent=_ru;';
  h += '    document.getElementById("refBox").style.display="block";';
  h += '    document.getElementById("impToken").value=_ru;';
  h += '    btn.disabled=false;btn.textContent="Refresh Again";';
  h += '  }).catch(function(e){alert("Error: "+e.message);btn.disabled=false;btn.textContent="Refresh Existing URL";});';
  h += '}';

  h += 'function copyRef(){';
  h += '  if(!_ru)return;';
  h += '  navigator.clipboard.writeText(_ru).then(function(){var b=document.getElementById("copyRefBtn");b.textContent="Copied!";setTimeout(function(){b.textContent="Copy URL";},1500);});';
  h += '}';

  h += 'function checkHealth(){';
  h += '  var list=document.getElementById("instList");';
  h += '  list.innerHTML="<div style=\\"color:#333;font-size:13px\\">Checking...</div>";';
  h += '  fetch("/instances").then(function(r){return r.json();})';
  h += '  .then(function(data){';
  h += '    list.innerHTML="";';
  h += '    (data.instances||[]).forEach(function(inst){';
  h += '      var row=document.createElement("div");row.className="inst";';
  h += '      var dot=document.createElement("span");dot.className=inst.ok?"dot ok":"dot err";';
  h += '      var urlSpan=document.createElement("span");urlSpan.className="inst-url";urlSpan.textContent=inst.url;';
  h += '      row.appendChild(dot);row.appendChild(urlSpan);';
  h += '      if(inst.ok){var ms=document.createElement("span");ms.className="inst-ms";ms.textContent=inst.ms+"ms";row.appendChild(ms);}';
  h += '      list.appendChild(row);';
  h += '    });';
  h += '  }).catch(function(){list.innerHTML="<div style=\\"color:#c04040;font-size:13px\\">Could not reach server</div>";});';
  h += '}';
  h += 'checkHealth();';

  h += 'function getTok(s){var m=s.match(/\\/u\\/([a-f0-9]{28})\\//);return m?m[1]:null;}';
  h += 'function hesc(s){return String(s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}';

  h += 'function doImport(type){';
  h += '  var raw=document.getElementById("impToken").value.trim();';
  h += '  var id=document.getElementById("impId").value.trim();';
  h += '  var st=document.getElementById("impStatus");';
  h += '  var pv=document.getElementById("impPreview");';
  h += '  if(!raw){st.className="status err";st.textContent="Paste your addon URL first.";return;}';
  h += '  if(!id){st.className="status err";st.textContent="Enter an album ID or playlist UUID.";return;}';
  h += '  var tok=getTok(raw);';
  h += '  if(!tok){st.className="status err";st.textContent="Could not find your token in the URL.";return;}';
  h += '  var btns=[document.getElementById("impAlbBtn"),document.getElementById("impPlBtn")];';
  h += '  btns.forEach(function(b){b.disabled=true;});';
  h += '  st.className="status";st.textContent="Fetching...";pv.style.display="none";';
  h += '  fetch("/u/"+tok+"/import-csv?type="+type+"&id="+encodeURIComponent(id))';
  h += '  .then(function(r){if(!r.ok)return r.json().then(function(e){throw new Error(e.error||"Server error "+r.status);});return r.json();})';
  h += '  .then(function(data){';
  h += '    var tracks=data.tracks||[];';
  h += '    if(!tracks.length)throw new Error("No tracks found.");';
  h += '    var rows=tracks.slice(0,50).map(function(t,i){return "<div class=\\"tr\\"><span class=\\"tn\\">"+(i+1)+"</span><div class=\\"ti\\"><div class=\\"tt\\">"+hesc(t.title)+"</div><div class=\\"ta\\">"+hesc(t.artist)+"</div></div></div>";}).join("");';
  h += '    if(tracks.length>50)rows+="<div class=\\"tr\\" style=\\"text-align:center;color:#333\\">+"+(tracks.length-50)+" more</div>";';
  h += '    pv.innerHTML=rows;pv.style.display="block";';
  h += '    st.className="status ok";st.textContent="Found "+tracks.length+" tracks in \\""+hesc(data.title)+"\\"";';
  h += '    var lines=["Title,Artist,Album,Duration"];';
  h += '    tracks.forEach(function(t){';
  h += '      function ce(s){s=String(s||"");if(s.indexOf(",")!==-1||s.indexOf("\\"")!==-1){s=\'"\'+s.replace(/"/g,\'""\')+\'"\';}return s;}';
  h += '      lines.push(ce(t.title)+","+ce(t.artist)+","+ce(data.title||"")+","+ce(t.duration||""));';
  h += '    });';
  h += '    var blob=new Blob([lines.join("\\n")],{type:"text/csv"});';
  h += '    var a=document.createElement("a");a.href=URL.createObjectURL(blob);';
  h += '    a.download=(data.title||"playlist").replace(/[^a-zA-Z0-9 _-]/g,"").trim()+".csv";';
  h += '    document.body.appendChild(a);a.click();document.body.removeChild(a);';
  h += '    btns.forEach(function(b){b.disabled=false;});';
  h += '  }).catch(function(e){st.className="status err";st.textContent=e.message;btns.forEach(function(b){b.disabled=false;});});';
  h += '}';

  h += '<\/script></body></html>';
  return h;
}

// ─── Routes ──────────────────────────────────────────────────────────────────

app.get('/', function(req, res) {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(buildConfigPage(getBaseUrl(req)));
});

app.post('/generate', async function(req, res) {
  var ip     = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown').split(',')[0].trim();
  var bucket = getOrCreateIpBucket(ip);
  if (bucket.count >= MAX_TOKENS_PER_IP) return res.status(429).json({ error: 'Too many tokens from this IP today.' });
  var token = generateToken();
  var entry = { createdAt: Date.now(), lastUsed: Date.now(), reqCount: 0, rateWin: [] };
  TOKEN_CACHE.set(token, entry);
  await redisSave(token, entry);
  bucket.count++;
  res.json({ token: token, manifestUrl: getBaseUrl(req) + '/u/' + token + '/manifest.json' });
});

app.post('/refresh', async function(req, res) {
  var raw   = (req.body && req.body.existingUrl) ? String(req.body.existingUrl).trim() : '';
  var token = raw;
  var m     = raw.match(/\/u\/([a-f0-9]{28})\//);
  if (m) token = m[1];
  if (!token || !/^[a-f0-9]{28}$/.test(token)) return res.status(400).json({ error: 'Paste your full addon URL.' });
  var entry = await getTokenEntry(token);
  if (!entry) return res.status(404).json({ error: 'URL not found. Generate a new one.' });
  res.json({ token: token, manifestUrl: getBaseUrl(req) + '/u/' + token + '/manifest.json', refreshed: true });
});

app.get('/instances', async function(_req, res) {
  var results = await Promise.all(HIFI_INSTANCES.map(async function(inst) {
    var start = Date.now();
    try {
      await axios.get(inst + '/search/', { params: { s: 'test', limit: 1 }, timeout: 6000 });
      return { url: inst, ok: true, ms: Date.now() - start };
    } catch (e) { return { url: inst, ok: false, ms: null }; }
  }));
  res.json({ instances: results });
});

app.get('/health', function(_req, res) {
  res.json({
    status:          'ok',
    version:         '1.3.0',
    hifiApiVersion:  '2.7',
    activeInstance:  activeInstance,
    instanceHealthy: instanceHealthy,
    activeTokens:    TOKEN_CACHE.size,
    redisConnected:  !!(redis && redis.status === 'ready'),
    timestamp:       new Date().toISOString()
  });
});

app.get('/u/:token/manifest.json', tokenMiddleware, function(req, res) {
  res.json({
    id:          'com.eclipse.claudochrome.' + req.params.token.slice(0, 8),
    name:        'Claudochrome (TIDAL)',
    version:     '1.3.0',
    description: 'Full TIDAL catalog via Hi-Fi API v2.7. Lossless FLAC, AAC 320. No account required.',
    icon:        'https://monochrome.tf/favicon.ico',
    resources:   ['search', 'stream', 'catalog'],
    types:       ['track', 'album', 'artist', 'playlist']
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// SEARCH
// v2.7: GET /search/?s=<term>&limit=20
// Returns tracks only regardless of flags. We extract albums/artists from them.
// ─────────────────────────────────────────────────────────────────────────────
app.get('/u/:token/search', tokenMiddleware, async function(req, res) {
  var q = String(req.query.q || '').trim();
  if (!q) return res.json({ tracks: [], albums: [], artists: [], playlists: [] });
  try {
    var data  = await hifiGet('/search/', { s: q, limit: 20, offset: 0 });
    var items = (data.data && data.data.items) || [];

    // Deduplicate albums and artists from track results
    var albumMap  = {};
    var artistMap = {};
    items.forEach(function(t) {
      if (t.album && t.album.id && !albumMap[t.album.id]) {
        albumMap[t.album.id] = {
          id:         String(t.album.id),
          title:      t.album.title || 'Unknown',
          artworkURL: coverUrl(t.album.cover)
        };
      }
      var arts = t.artists || (t.artist ? [t.artist] : []);
      arts.forEach(function(a) {
        if (a && a.id && !artistMap[a.id]) {
          artistMap[a.id] = {
            id:         String(a.id),
            name:       a.name || 'Unknown',
            artworkURL: coverUrl(a.picture, 320)
          };
        }
      });
    });

    res.json({
      tracks: items
        .filter(function(t) { return t.streamReady !== false && t.allowStreaming !== false; })
        .map(function(t) {
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
      albums:   Object.values(albumMap).slice(0, 8),
      artists:  Object.values(artistMap).slice(0, 5),
      playlists: []
    });
  } catch (e) {
    console.error('[search] ' + e.message);
    res.status(502).json({ error: 'Search failed: ' + e.message, tracks: [] });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// STREAM
// v2.7: GET /track/?id=<id>&quality=LOSSLESS
// Returns a base64 manifest. Decode it to get the real CDN URL.
// ─────────────────────────────────────────────────────────────────────────────
app.get('/u/:token/stream/:id', tokenMiddleware, async function(req, res) {
  var tid      = req.params.id;
  var qualities = ['LOSSLESS', 'HIGH', 'LOW'];
  for (var q of qualities) {
    try {
      var data    = await hifiGet('/track/', { id: tid, quality: q });
      var payload = data.data || data;

      if (payload && payload.manifest) {
        var decoded = decodeManifest(payload.manifest);
        if (decoded && decoded.url) {
          var isFlac = decoded.codec && (decoded.codec.indexOf('flac') !== -1 || decoded.codec.indexOf('audio/flac') !== -1);
          return res.json({
            url:       decoded.url,
            format:    isFlac ? 'flac' : 'aac',
            quality:   q === 'LOSSLESS' ? 'lossless' : (q === 'HIGH' ? '320kbps' : '128kbps'),
            expiresAt: Math.floor(Date.now() / 1000) + 21600
          });
        }
      }

      // Some instances return url directly (fallback)
      if (payload && payload.url) {
        return res.json({
          url:       payload.url,
          format:    'aac',
          quality:   'lossless',
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

// ─────────────────────────────────────────────────────────────────────────────
// ALBUM
// v2.7: GET /album/?id=<id>&limit=100
// Response shape: data.data = album object, data.data.items[].item = track
// ─────────────────────────────────────────────────────────────────────────────
app.get('/u/:token/album/:id', tokenMiddleware, async function(req, res) {
  var aid = req.params.id;
  try {
    var data     = await hifiGet('/album/', { id: aid, limit: 100, offset: 0 });
    var album    = data.data || data;                 // top-level album object
    var rawItems = album.items || [];                 // items[].item = track

    var artistName = 'Unknown';
    if (album.artist && album.artist.name)   artistName = album.artist.name;
    else if (album.artists && album.artists.length) {
      artistName = album.artists.map(function(a) { return a.name; }).join(', ');
    }

    res.json({
      id:         String(album.id || aid),
      title:      album.title || 'Unknown',
      artist:     artistName,
      artworkURL: coverUrl(album.cover, 640),
      year:       (album.releaseDate || '').slice(0, 4) || null,
      trackCount: album.numberOfTracks || rawItems.length,
      tracks: rawItems
        .map(function(entry) { return entry.item || entry; })   // unwrap nested .item
        .filter(function(t)  { return t && t.id && t.streamReady !== false; })
        .map(function(t, idx) {
          return {
            id:          String(t.id),
            title:       t.title || 'Unknown',
            artist:      trackArtist(t) || artistName,
            duration:    trackDuration(t),
            trackNumber: t.trackNumber || (idx + 1),
            artworkURL:  coverUrl(album.cover)
          };
        })
    });
  } catch (e) {
    console.error('[album] ' + e.message);
    res.status(502).json({ error: 'Album fetch failed: ' + e.message });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// ARTIST
// v2.7 requires TWO calls:
//   GET /artist/?id=<id>               → basic info + cover URL  (data.artist, data.cover)
//   GET /artist/?f=<id>&skip_tracks=false → full discography     (data.albums.items, data.tracks)
//
// IMPORTANT: the 'f' param is the artist ID as an integer — NOT a type string
// ─────────────────────────────────────────────────────────────────────────────
app.get('/u/:token/artist/:id', tokenMiddleware, async function(req, res) {
  var aid = parseInt(req.params.id, 10);
  if (isNaN(aid)) return res.status(400).json({ error: 'Invalid artist ID' });
  try {
    var results = await Promise.all([
      hifiGet('/artist/', { id: aid }),                             // basic info
      hifiGet('/artist/', { f: aid, skip_tracks: false })           // full discography
    ]);

    var infoData   = results[0];
    var discData   = results[1];
    var artistInfo = infoData.artist || {};
    var coverData  = infoData.cover  || {};       // has key "750" with direct image URL
    var albumItems = (discData.albums && discData.albums.items) || [];
    var allTracks  = discData.tracks || [];

    // coverData["750"] is a full https URL — use it directly
    var artworkURL = coverData['750'] || coverUrl(artistInfo.picture, 480);

    // Top 20 tracks sorted by popularity descending
    var topTracks = allTracks
      .filter(function(t) { return t.streamReady !== false && t.allowStreaming !== false; })
      .sort(function(a, b) { return (b.popularity || 0) - (a.popularity || 0); })
      .slice(0, 20)
      .map(function(t) {
        return {
          id:         String(t.id),
          title:      t.title || 'Unknown',
          artist:     trackArtist(t),
          duration:   trackDuration(t),
          artworkURL: coverUrl(t.album && t.album.cover)
        };
      });

    // Albums sorted newest first, capped at 60
    var albums = albumItems
      .filter(function(a) { return a.streamReady !== false && a.allowStreaming !== false; })
      .sort(function(a, b) { return (b.releaseDate || '').localeCompare(a.releaseDate || ''); })
      .slice(0, 60)
      .map(function(a) {
        return {
          id:         String(a.id),
          title:      a.title || 'Unknown',
          artworkURL: coverUrl(a.cover),
          trackCount: a.numberOfTracks || null,
          year:       (a.releaseDate || '').slice(0, 4) || null
        };
      });

    res.json({
      id:         String(artistInfo.id || aid),
      name:       artistInfo.name || 'Unknown',
      artworkURL: artworkURL,
      bio:        null,
      topTracks:  topTracks,
      albums:     albums
    });
  } catch (e) {
    console.error('[artist] ' + e.message);
    res.status(502).json({ error: 'Artist fetch failed: ' + e.message });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// PLAYLIST
// v2.7: GET /playlist/?id=<uuid>&limit=100
// Response shape: data.playlist = playlist info, data.items[].item = track
// ─────────────────────────────────────────────────────────────────────────────
app.get('/u/:token/playlist/:id', tokenMiddleware, async function(req, res) {
  var pid = req.params.id;
  try {
    var data = await hifiGet('/playlist/', { id: pid, limit: 100, offset: 0 });

    // Top-level keys: version, playlist, items
    var pl       = data.playlist || data.data || data;
    var rawItems = data.items || (pl.items) || [];

    // Each raw item: { item: trackObject, type: "track" } — same shape as album
    var tracks = rawItems
      .map(function(entry) { return entry.item || entry; })
      .filter(function(t)  { return t && t.id && t.streamReady !== false; })
      .map(function(t) {
        return {
          id:         String(t.id),
          title:      t.title || 'Unknown',
          artist:     trackArtist(t),
          duration:   trackDuration(t),
          artworkURL: coverUrl(t.album && t.album.cover)
        };
      });

    // Cover: playlists use squareImage UUID
    var artworkURL = null;
    if (pl.squareImage) artworkURL = coverUrl(pl.squareImage, 480);

    res.json({
      id:         String(pl.uuid || pl.id || pid),
      title:      pl.title || 'Playlist',
      creator:    (pl.creator && pl.creator.name) || null,
      artworkURL: artworkURL,
      trackCount: pl.numberOfTracks || tracks.length,
      tracks:     tracks
    });
  } catch (e) {
    console.error('[playlist] ' + e.message);
    res.status(502).json({ error: 'Playlist fetch failed: ' + e.message });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// IMPORT CSV  (album or playlist → downloadable CSV for Eclipse library)
// ─────────────────────────────────────────────────────────────────────────────
app.get('/u/:token/import-csv', tokenMiddleware, async function(req, res) {
  var type = req.query.type;
  var id   = String(req.query.id || '').trim();
  if (!type || !id) return res.status(400).json({ error: 'Pass ?type=album|playlist&id=...' });

  try {
    if (type === 'album') {
      var d    = await hifiGet('/album/', { id: id, limit: 100 });
      var album = d.data || d;
      var raw   = album.items || [];
      return res.json({
        title:  album.title || 'Album',
        tracks: raw
          .map(function(e) { return e.item || e; })
          .filter(function(t) { return t && t.id && t.streamReady !== false; })
          .map(function(t) {
            return { title: t.title || 'Unknown', artist: trackArtist(t), duration: trackDuration(t) };
          })
      });
    }

    if (type === 'playlist') {
      var d2      = await hifiGet('/playlist/', { id: id, limit: 100 });
      var pl2     = d2.playlist || d2.data || d2;
      var rawPl   = d2.items || pl2.items || [];
      return res.json({
        title:  pl2.title || 'Playlist',
        tracks: rawPl
          .map(function(e) { return e.item || e; })
          .filter(function(t) { return t && t.id && t.streamReady !== false; })
          .map(function(t) {
            return { title: t.title || 'Unknown', artist: trackArtist(t), duration: trackDuration(t) };
          })
      });
    }

    res.status(400).json({ error: 'type must be album or playlist.' });
  } catch (e) {
    res.status(502).json({ error: 'Import failed: ' + e.message });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
app.listen(PORT, function() {
  console.log('Claudochrome v1.3.0 (Hi-Fi API v2.7) on port ' + PORT);
});
