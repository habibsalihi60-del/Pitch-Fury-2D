/* Pitch Fury online server: ranked matchmaking (Elo MMR), private rooms, leaderboard, validated relay.
   The HOST client runs the match; this server pairs players, enforces roles, sanitises squads, rate-limits,
   and settles ranked results. Run: npm install && npm start (PORT env, default 8080). */
const http = require('http');
const fs = require('fs');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean); // empty = allow all
const DATA_FILE = process.env.DATA_FILE || './players.json';      // on hosts with ephemeral disks, use a mounted volume or a DB
const MIN_MATCH_SEC = +(process.env.MIN_MATCH_SEC || 60);        // results of shorter matches never change ratings
const MAX_MSG_PER_SEC = 200;
const START_MMR = 1000, WIN_BASE = 60, WIN_GROW = 30;            // search window = 60 + 30 per second waiting
const PAIR_LIMIT = 3;                                            // rated games between the same two players per hour

/* ---------- persistence ---------- */
let DB = {}; try { DB = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch (e) { }
let dirty = false;
setInterval(() => { if (!dirty) return; dirty = false; fs.writeFile(DATA_FILE, JSON.stringify(DB), () => { }); }, 10000);
const player = uid => DB[uid] || (DB[uid] = { name: '', mmr: START_MMR, g: 0, w: 0, l: 0, d: 0 });
const pub = p => ({ mmr: p.mmr, g: p.g, w: p.w, l: p.l, d: p.d });
const rankOf = p => 1 + Object.values(DB).filter(q => q.g > 0 && q.mmr > p.mmr).length;

/* ---------- http / ws ---------- */
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); return res.end('ok'); }
  res.writeHead(404); res.end();
});
/* no per-message compression: snapshots are already deltas, and deflate adds latency and CPU on small hosts */
const wss = new WebSocketServer({ server, maxPayload: 64 * 1024, perMessageDeflate: false });

let nextId = 1;
const queue = [];
const rooms = new Map();
const recent = new Map();      // "uidA|uidB" -> [timestamps of rated games]
const ALPHA = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
const code4 = () => { for (let n = 0; n < 50; n++) { let c = ''; for (let i = 0; i < 4; i++) c += ALPHA[Math.random() * ALPHA.length | 0]; if (!rooms.has(c)) return c; } return null; };

const clean = (v, n) => String(v == null ? '' : v).replace(/[\u0000-\u001f\u007f<>]/g, '').slice(0, n);
const num = (v, lo, hi, d) => { v = +v; return Number.isFinite(v) ? Math.max(lo, Math.min(hi, Math.round(v))) : d; };
const color = v => /^#[0-9a-fA-F]{3,8}$/.test(String(v)) ? String(v) : '#888888';
const POS = new Set(['G', 'D', 'M', 'A', 'LB', 'RB', 'CB', 'LM', 'RM', 'CM', 'ST', 'W']);

function sanitizeInfo(i) {
  if (!i || !Array.isArray(i.pl) || i.pl.length !== 6 || !Array.isArray(i.tn)) return null;
  const pl = i.pl.map(p => p && ({
    name: clean(p.name, 26), pos: POS.has(String(p.pos)) ? String(p.pos) : clean(p.pos, 3),
    ovr: num(p.ovr, 1, 130, 50), pac: num(p.pac, 1, 130, 50), sho: num(p.sho, 1, 130, 50),
    pas: num(p.pas, 1, 130, 50), def: num(p.def, 1, 130, 50), dri: num(p.dri, 1, 130, 50), xp: 0,
  }));
  if (pl.some(p => !p || !p.name)) return null;
  return { tn: [clean(i.tn[0], 24), color(i.tn[1]), color(i.tn[2])], pl };
}
const send = (c, o) => { if (c && c.ws.readyState === 1) c.ws.send(JSON.stringify(o)); };

/* ---------- ratings (Elo, bigger steps while a player is new) ---------- */
const kFor = p => p.g < 10 ? 40 : p.g < 30 ? 28 : 20;
function elo(p, oppMmr, S, gd, capped) {
  if (capped) return 0;
  const E = 1 / (1 + Math.pow(10, (oppMmr - p.mmr) / 400));
  const mult = S === 1 ? 1 + 0.1 * Math.min(Math.max(gd - 1, 0), 3) : 1;
  return Math.round(kFor(p) * (S - E) * mult);
}
/* sa = result for side a: 1 win, .5 draw, 0 loss */
function settle(match, sa, gd) {
  if (match.done) return; match.done = true; match.live = false;
  const A = match.a, B = match.b; if (!A.p || !B.p) return;
  const key = [A.uid, B.uid].sort().join('|'), now = Date.now();
  const hist = (recent.get(key) || []).filter(t => now - t < 3600e3), capped = hist.length >= PAIR_LIMIT;
  if (!capped) { hist.push(now); } recent.set(key, hist);
  const am = A.p.mmr, bm = B.p.mmr;
  const da = elo(A.p, bm, sa, gd, capped), db = elo(B.p, am, 1 - sa, gd, capped);
  for (const [c, d, s] of [[A, da, sa], [B, db, 1 - sa]]) {
    c.p.mmr = Math.max(100, c.p.mmr + d); c.p.g++; if (s === 1) c.p.w++; else if (s === 0) c.p.l++; else c.p.d++;
    send(c, { t: 'rated', delta: d, capped, ...pub(c.p), rank: rankOf(c.p) });
  }
  dirty = true;
}
/* host reports the final score: a = host */
function settleScore(match, sc, pw) {
  const a = num(sc && sc[0], 0, 99, 0), b = num(sc && sc[1], 0, 99, 0);
  if (Date.now() - match.t0 < MIN_MATCH_SEC * 1000) { match.done = true; match.live = false; send(match.a, { t: 'rated', delta: 0, capped: true, ...pub(match.a.p), rank: rankOf(match.a.p) }); send(match.b, { t: 'rated', delta: 0, capped: true, ...pub(match.b.p), rank: rankOf(match.b.p) }); return; }
  const hostSide = match.a.role === 'host' ? match.a : match.b;
  let s = a > b ? 1 : a < b ? 0 : (pw === 0 ? 1 : pw === 1 ? 0 : .5);   // from the host's side
  settle(match, hostSide === match.a ? s : 1 - s, Math.abs(a - b));
}

/* ---------- pairing ---------- */
function unlink(c, notify) {
  const p = c.partner; c.partner = null; c.match = null;
  if (p) { p.partner = null; p.match = null; if (notify) send(p, { t: 'left' }); }
}
function leaveAll(c, notify) {
  const qi = queue.indexOf(c); if (qi >= 0) queue.splice(qi, 1);
  if (c.room) { if (rooms.get(c.room) === c) rooms.delete(c.room); c.room = null; }
  const m = c.match;   // leaving a live ranked match = a loss
  if (m && m.ranked && m.live && !m.done) settle(m, m.a === c ? 0 : 1, 1);
  unlink(c, notify);
}
function pair(host, guest, ranked) {
  leaveAll(host, false); leaveAll(guest, false);
  const match = { a: host, b: guest, ranked, live: false, done: false, roles: 0, t0: Date.now() };
  host.match = guest.match = match; host.partner = guest; guest.partner = host; host.role = 'host'; guest.role = 'guest';
  send(host, { t: 'matched', role: 'host', opp: guest.name, ranked, oppMmr: ranked ? guest.p.mmr : 0 });
  send(guest, { t: 'matched', role: 'guest', opp: host.name, ranked, oppMmr: ranked ? host.p.mmr : 0 });
}
setInterval(() => {
  const now = Date.now(); queue.sort((a, b) => a.since - b.since);
  for (let i = 0; i < queue.length; i++) {
    const a = queue[i];
    for (let j = i + 1; j < queue.length; j++) {
      const b = queue[j]; if (a.uid && a.uid === b.uid) continue;
      const w = Math.min(WIN_BASE + WIN_GROW * (now - a.since) / 1000, WIN_BASE + WIN_GROW * (now - b.since) / 1000);
      if (Math.abs(a.p.mmr - b.p.mmr) <= w) { pair(a, b, true); i = -1; break; }   // longest waiter hosts
    }
  }
  for (const c of queue) send(c, { t: 'q', range: Math.round(WIN_BASE + WIN_GROW * (now - c.since) / 1000), n: queue.length });
}, 1000);

const HOST_ONLY = new Set(['start', 's', 'over']), GUEST_ONLY = new Set(['hello', 'in', 'joy']), EITHER = new Set(['rm', 'nonce']);

wss.on('connection', (ws, req) => {
   wss.on('connection', (ws, req) => {
  req.socket.setNoDelay(true);   // send small packets immediately
  ...
  if (ORIGINS.length && !ORIGINS.some(o => String(req.headers.origin || '').endsWith(o))) { ws.close(1008, 'origin'); return; }
  ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
  const c = { id: nextId++, ws, uid: '', p: null, name: '', role: '', partner: null, match: null, room: null, since: 0, n: 0, t0: Date.now() };
  ws.on('message', raw => {
    const now = Date.now(); if (now - c.t0 >= 1000) { c.t0 = now; c.n = 0; } if (++c.n > MAX_MSG_PER_SEC) return;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    if (m.name !== undefined) { c.name = clean(m.name, 16); if (c.p) c.p.name = c.name; }
    switch (m.t) {
      case 'auth': {
        const uid = clean(m.uid, 64); if (uid.length < 8) return;
        c.uid = uid; c.p = player(uid); if (c.name) c.p.name = c.name; dirty = true;
        send(c, { t: 'me', ...pub(c.p), rank: c.p.g ? rankOf(c.p) : 0 }); break;
      }
      case 'find':
        if (c.partner) return; if (!c.p) return send(c, { t: 'err', m: 'Sign-in failed, reconnect and try again.' });
        leaveAll(c, false); c.since = Date.now(); queue.push(c); send(c, { t: 'queued', mmr: c.p.mmr }); break;
      case 'create': {
        if (c.partner) return; leaveAll(c, false);
        const code = code4(); if (!code) return send(c, { t: 'err', m: 'Server is busy, try again.' });
        c.room = code; rooms.set(code, c); send(c, { t: 'room', code }); break;
      }
      case 'join': {
        if (c.partner) return; const code = clean(m.code, 4).toUpperCase(), h = rooms.get(code);
        if (!h || h === c || h.partner) return send(c, { t: 'err', m: 'Room not found or already full.' });
        pair(h, c, false); break;
      }
      case 'lb': {
        const top = Object.values(DB).filter(q => q.g > 0).sort((a, b) => b.mmr - a.mmr).slice(0, 20).map(q => ({ n: q.name || 'Player', mmr: q.mmr, w: q.w, l: q.l, d: q.d }));
        send(c, { t: 'lb', top, me: c.p ? { ...pub(c.p), rank: c.p.g ? rankOf(c.p) : 0 } : null }); break;
      }
      case 'ping': send(c, { t: 'pong', ts: num(m.ts, 0, 9e15, 0) }); break;
      case 'cancel': leaveAll(c, true); break;
      case 'r': {
        const p = c.partner, d = m.d; if (!p || !d || typeof d.k !== 'string') return;
        /* clients agree on who hosts (random-number handshake); the first 'hello' tells us who the guest is, after that roles are enforced */
        if (d.k === 'hello' && c.match && !c.match.roles) { c.role = 'guest'; p.role = 'host'; c.match.roles = 1; }
        if (d.k === 'nonce') { d.n = +d.n || 0; }
        if (HOST_ONLY.has(d.k) ? c.role !== 'host' : GUEST_ONLY.has(d.k) ? c.role !== 'guest' : !EITHER.has(d.k)) return;
        if (d.k === 'hello') { d.i = sanitizeInfo(d.i); if (!d.i) return; }
        if (d.k === 'start') { d.h = sanitizeInfo(d.h); d.g = sanitizeInfo(d.g); if (!d.h || !d.g) return; if (c.match) { c.match.live = true; c.match.done = false; c.match.t0 = Date.now(); } }
        if (d.k === 'in') { d.a = clean(d.a, 16); d.d = d.d ? 1 : 0; }
        if (d.k === 'over' && c.match && c.match.ranked && c.match.live && !c.match.done) settleScore(c.match, d.sc, d.pw);
        send(p, { t: 'r', d }); break;
      }
    }
  });
  ws.on('close', () => leaveAll(c, true));
  ws.on('error', () => { });
});
setInterval(() => { wss.clients.forEach(ws => { if (ws.isAlive === false) return ws.terminate(); ws.isAlive = false; ws.ping(); }); }, 20000);

server.listen(PORT, () => console.log('Pitch Fury server listening on :' + PORT));
