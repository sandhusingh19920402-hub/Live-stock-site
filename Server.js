const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const KEY = process.env.ALPACA_KEY, SECRET = process.env.ALPACA_SECRET;
const FEED = process.env.FEED || "iex";
const headers = { "APCA-API-KEY-ID": KEY, "APCA-API-SECRET-KEY": SECRET };
const clean = s => String(s || "").toUpperCase().replace(/[^A-Z.]/g, "").slice(0, 8);

const app = express();
app.use(express.static("public"));

// History + previous close
app.get("/api/bars", async (req, res) => {
  try {
    const sym = clean(req.query.symbol) || "AAPL";
    const tf = ["1Min", "5Min", "15Min"].includes(req.query.tf) ? req.query.tf : "1Min";
    const start = new Date(Date.now() - 6 * 86400000).toISOString();
    const base = `https://data.alpaca.markets/v2/stocks/${sym}/bars`;
    const [a, d] = await Promise.all([
      fetch(`${base}?timeframe=${tf}&start=${start}&limit=400&sort=desc&feed=${FEED}`, { headers }),
      fetch(`${base}?timeframe=1Day&start=${start}&limit=3&sort=desc&feed=${FEED}`, { headers })
    ]);
    const bars = await a.json(), daily = await d.json();
    if (!a.ok) return res.status(a.status).json({ error: bars.message || "Alpaca error" });
    res.json({
      bars: (bars.bars || []).reverse(),
      prevClose: (daily.bars || []).length > 1 ? daily.bars[1].c : null
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// One upstream Alpaca connection, shared by all visitors
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/live" });
const subs = new Map();   // symbol -> Set of browser sockets
let up, ready = false;

function connectUp() {
  up = new WebSocket(`wss://stream.data.alpaca.markets/v2/${FEED}`);
  up.on("message", data => {
    for (const m of JSON.parse(data)) {
      if (m.T === "success" && m.msg === "connected")
        up.send(JSON.stringify({ action: "auth", key: KEY, secret: SECRET }));
      else if (m.T === "success" && m.msg === "authenticated") {
        ready = true;
        const s = [...subs.keys()];
        if (s.length) up.send(JSON.stringify({ action: "subscribe", trades: s }));
      } else if (m.T === "t") {
        const set = subs.get(m.S);
        if (set) {
          const out = JSON.stringify({ p: m.p, s: m.s, t: m.t });
          set.forEach(c => c.readyState === 1 && c.send(out));
        }
      }
    }
  });
  up.on("close", () => { ready = false; setTimeout(connectUp, 5000); });
  up.on("error", () => {});
}
connectUp();

wss.on("connection", ws => {
  let sym = null;
  const drop = () => {
    const set = sym && subs.get(sym);
    if (set) {
      set.delete(ws);
      if (!set.size) {
        subs.delete(sym);
        if (ready) up.send(JSON.stringify({ action: "unsubscribe", trades: [sym] }));
      }
    }
    sym = null;
  };
  ws.on("message", d => {
    let m; try { m = JSON.parse(d); } catch { return; }
    const s = clean(m.symbol);
    if (!s) return;
    drop();
    sym = s;
    if (!subs.has(s)) {
      subs.set(s, new Set());
      if (ready) up.send(JSON.stringify({ action: "subscribe", trades: [s] }));
    }
    subs.get(s).add(ws);
  });
  ws.on("close", drop);
});

server.listen(process.env.PORT || 3000, () => console.log("Running"));
