// Pitch Fury matchmaking server. No dependencies. Run: node server.js  (PORT env, default 8080)
// Client: open the game with ?server=ws://localhost:8080, or set ONLINE_SERVER in the HTML.
const http = require('http'), crypto = require('crypto'), fs = require('fs');
const PORT = +process.env.PORT || 8080, DB = process.env.DB || './ratings.json';
let R = {}; try { R = JSON.parse(fs.readFileSync(DB, 'utf8')); } catch (e) { }
let dirty = 0; setInterval(() => { if (dirty) { dirty = 0; try { fs.writeFileSync(DB, JSON.stringify(R)); } catch (e) { } } }, 5000);
const rec = uid => R[uid] || (R[uid] = { n: 'Player', mmr: 1000, g: 0, w: 0, d: 0, l: 0, vs: {} });
const rank = uid => 1 + Object.values(R).filter(r => r.g && r.mmr > R[uid].mmr).length;
const me = uid => { const r = rec(uid); return { t: 'me', mmr: r.mmr, g: r.g, w: r.w, d: r.d, l: r.l, rank: r.g ? rank(uid) : 0 }; };

/* minimal RFC 6455 websocket (text frames only) */
function frame(s) { const b = Buffer.from(s), n = b.length; let h; if (n < 126) h = Buffer.from([0x81, n]); else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x81; h[1] = 126; h.writeUInt16BE(n, 2); } else { h = Buffer.alloc(10); h[0] = 0x81; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); } return Buffer.concat([h, b]); }
const server = http.createServer((q, s) => { s.writeHead(200); s.end('Pitch Fury server'); });
const queue = [], rooms = {};
function send(c, o) { if (c && !c.dead) try { c.sock.write(frame(JSON.stringify(o))); } catch (e) { } }
function pair(a, b, ranked) {
    a.opp = b; b.opp = a; a.q = b.q = 0; a.over = b.over = 0; a.ranked = b.ranked = ranked; a.role = 'host'; b.role = 'guest';
    send(a, { t: 'matched', role: 'host', opp: b.name, ranked, oppMmr: rec(b.uid).mmr });
    send(b, { t: 'matched', role: 'guest', opp: a.name, ranked, oppMmr: rec(a.uid).mmr });
}
function leave(c) {
    const i = queue.indexOf(c); if (i >= 0) queue.splice(i, 1); c.q = 0;
    if (c.code && rooms[c.code] === c) delete rooms[c.code]; c.code = '';
    if (c.opp) { const o = c.opp; c.opp = null; if (o.opp === c) { o.opp = null; send(o, { t: 'left' }); } }
}
function rate(host, guest, sc, pw) {
    if (host.over) return; host.over = guest.over = 1; if (!host.ranked) return;
    const [a, b] = sc, ra = rec(host.uid), rb = rec(guest.uid);
    const sa = a > b ? 1 : a < b ? 0 : pw === 0 ? 1 : pw === 1 ? 0 : .5;
    const ea = 1 / (1 + Math.pow(10, (rb.mmr - ra.mmr) / 400)), K = 24;
    const key = [host.uid, guest.uid].sort().join('|'), day = Math.floor(Date.now() / 864e5);
    ra.vs[guest.uid] = (ra.vs[guest.uid] && ra.vs[guest.uid][0] === day ? ra.vs[guest.uid] : [day, 0]); const cnt = ++ra.vs[guest.uid][1];
    const capped = cnt > 5 || a + b > 40; let da = 0, db = 0;
    if (!capped) { da = Math.round(K * (sa - ea)); db = -da; ra.mmr = Math.max(0, ra.mmr + da); rb.mmr = Math.max(0, rb.mmr + db); }
    ra.g++; rb.g++; if (sa === 1) { ra.w++; rb.l++; } else if (sa === 0) { ra.l++; rb.w++; } else { ra.d++; rb.d++; }
    dirty = 1;
    [[host, ra, da], [guest, rb, db]].forEach(([c, r, d]) => send(c, { t: 'rated', mmr: r.mmr, g: r.g, w: r.w, d: r.d, l: r.l, rank: rank(c.uid), delta: d, capped }));
}
function onMsg(c, m) {
    if (m.t === 'auth') { c.uid = String(m.uid || '').slice(0, 64); if (c.uid.length < 8) return c.sock.destroy(); c.name = String(m.name || 'Player').slice(0, 16) || 'Player'; rec(c.uid).n = c.name; return send(c, me(c.uid)); }
    if (!c.uid) return;
    if (m.t === 'find') { leave(c); c.name = String(m.name || c.name).slice(0, 16) || 'Player'; rec(c.uid).n = c.name; c.q = Date.now(); queue.push(c); send(c, { t: 'queued' }); }
    else if (m.t === 'cancel') leave(c);
    else if (m.t === 'create') { leave(c); let k; do { k = Array.from({ length: 4 }, () => 'ABCDEFGHJKLMNPQRSTUVWXYZ'[Math.random() * 24 | 0]).join(''); } while (rooms[k]); rooms[k] = c; c.code = k; send(c, { t: 'room', code: k }); }
    else if (m.t === 'join') { const h = rooms[String(m.code || '').toUpperCase()]; if (!h || h === c || h.opp) return send(c, { t: 'err', m: 'Room not found.' }); leave(c); delete rooms[h.code]; h.code = ''; c.name = String(m.name || c.name).slice(0, 16) || 'Player'; pair(h, c, 0); }
    else if (m.t === 'lb') { const top = Object.values(R).filter(r => r.g).sort((a, b) => b.mmr - a.mmr).slice(0, 20).map(r => ({ n: r.n, mmr: r.mmr })); send(c, { t: 'lb', top, me: me(c.uid) }); }
    else if (m.t === 'r' && c.opp && m.d) {
        if (m.d.k === 'over' && c.role === 'host' && Array.isArray(m.d.sc)) rate(c, c.opp, m.d.sc.map(x => Math.max(0, +x | 0)), m.d.pw);
        send(c.opp, m);
    }
}
/* matchmaking tick: closest MMR first, range widens with wait time */
setInterval(() => {
    const now = Date.now();
    for (const c of queue) { c.range = 75 + Math.floor((now - c.q) / 2000) * 50; send(c, { t: 'q', range: c.range }); }
    for (let i = 0; i < queue.length; i++) for (let j = i + 1; j < queue.length; j++) {
        const a = queue[i], b = queue[j];
        if (Math.abs(rec(a.uid).mmr - rec(b.uid).mmr) <= Math.max(a.range, b.range) && a.uid !== b.uid) { queue.splice(j, 1); queue.splice(i, 1); pair(a, b, 1); i--; break; }
    }
}, 1000);
server.on('upgrade', (req, sock) => {
    const key = req.headers['sec-websocket-key']; if (!key) return sock.destroy();
    sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64') + '\r\n\r\n');
    const c = { sock, uid: '', name: 'Player', q: 0, opp: null, code: '', dead: 0 }; let buf = Buffer.alloc(0);
    sock.on('data', d => {
        buf = Buffer.concat([buf, d]);
        for (; ;) {
            if (buf.length < 2) return; const op = buf[0] & 15; let n = buf[1] & 127, o = 2;
            if (n === 126) { if (buf.length < 4) return; n = buf.readUInt16BE(2); o = 4; } else if (n === 127) { if (buf.length < 10) return; n = Number(buf.readBigUInt64BE(2)); o = 10; }
            if (n > 1 << 20) return sock.destroy(); if (buf.length < o + 4 + n) return;
            const mk = buf.slice(o, o + 4), p = Buffer.from(buf.slice(o + 4, o + 4 + n)); for (let i = 0; i < n; i++) p[i] ^= mk[i & 3]; buf = buf.slice(o + 4 + n);
            if (op === 8) return sock.end(); if (op === 9) { sock.write(Buffer.concat([Buffer.from([0x8a, p.length]), p])); continue; }
            if (op === 1) { try { onMsg(c, JSON.parse(p.toString())); } catch (e) { } }
        }
    });
    const bye = () => { if (c.dead) return; c.dead = 1; leave(c); }; sock.on('close', bye); sock.on('error', bye);
});
server.listen(PORT, () => console.log('Pitch Fury server on :' + PORT));
