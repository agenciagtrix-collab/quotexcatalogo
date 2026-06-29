// Quotex collector worker.
// Connects to Quotex WS, aggregates incoming ticks into 1m/5m/15m candles,
// and POSTs them in batches to the platform's /api/public/ingest endpoint.
//
// NOTE: This is the production skeleton. Plug your Quotex protocol details
// in `connectQuotex()` — the project README documents how to extract SSID
// and the Socket.IO frame shape.

import WebSocket from "ws";
import { signBody } from "./sign.js";

const INGEST_URL = required("INGEST_URL");
const INGEST_HMAC_SECRET = required("INGEST_HMAC_SECRET");
const QUOTEX_SSID = required("QUOTEX_SSID");
const ASSETS = required("QUOTEX_ASSETS").split(",").map((s) => s.trim());

function required(name) {
  const v = process.env[name];
  if (!v) {
    console.error(`Missing env ${name}`);
    process.exit(1);
  }
  return v;
}

const BATCH = [];
const FLUSH_MS = 2000;

async function flush() {
  if (BATCH.length === 0) return;
  const candles = BATCH.splice(0, BATCH.length);
  const body = JSON.stringify({ candles });
  const sig = signBody(body, INGEST_HMAC_SECRET);
  try {
    const res = await fetch(INGEST_URL, {
      method: "POST",
      headers: { "content-type": "application/json", "x-signature": sig },
      body,
    });
    const text = await res.text();
    console.log(`[ingest] ${res.status} ${text} (${candles.length} candles)`);
    if (!res.ok) BATCH.unshift(...candles); // requeue
  } catch (e) {
    console.error("[ingest] error", e.message);
    BATCH.unshift(...candles);
  }
}
setInterval(flush, FLUSH_MS);

/**
 * Aggregator: receives tick { symbol, ts, price } and emits closed candles.
 */
const buckets = new Map(); // key=symbol:tf:bucketTs -> {open,high,low,close}
function onTick({ symbol, ts, price }) {
  for (const tf of [1, 5, 15]) {
    const bucketMs = tf * 60_000;
    const bucketTs = Math.floor(ts / bucketMs) * bucketMs;
    const key = `${symbol}:${tf}:${bucketTs}`;
    let b = buckets.get(key);
    if (!b) {
      b = { open: price, high: price, low: price, close: price, opened_at: new Date(bucketTs).toISOString(), symbol, timeframe: tf, _bucketTs: bucketTs };
      buckets.set(key, b);
    } else {
      b.high = Math.max(b.high, price);
      b.low = Math.min(b.low, price);
      b.close = price;
    }
  }
  // Close candles whose bucket is older than now
  const now = Date.now();
  for (const [k, b] of buckets) {
    if (b._bucketTs + b.timeframe * 60_000 <= now) {
      BATCH.push({
        symbol: b.symbol,
        timeframe: b.timeframe,
        opened_at: b.opened_at,
        open: b.open, high: b.high, low: b.low, close: b.close,
      });
      buckets.delete(k);
    }
  }
}

/**
 * Connect to Quotex WS and subscribe to ASSETS.
 * The exact Socket.IO event shape changes over time; adjust here when needed.
 */
function connectQuotex() {
  const url = "wss://ws2.qxbroker.com/socket.io/?EIO=3&transport=websocket";
  const ws = new WebSocket(url, { headers: { Origin: "https://qxbroker.com" } });

  ws.on("open", () => {
    console.log("[quotex] connected");
    // Socket.IO handshake — auth right away
    ws.send(`42["authorization",${JSON.stringify({ session: QUOTEX_SSID, isDemo: 0, tournamentId: 0 })}]`);
    for (const symbol of ASSETS) {
      ws.send(`42["instruments/follow",${JSON.stringify({ asset: symbol, period: 60 })}]`);
    }
  });

  ws.on("message", (raw) => {
    const text = raw.toString();
    if (text === "2") { ws.send("3"); return; } // ping/pong
    if (!text.startsWith("42")) return;
    try {
      const payload = JSON.parse(text.slice(2));
      const [event, data] = payload;
      // Adjust event name when Quotex changes it
       // Adjust event name when Quotex changes it
      if (event === "instruments/update" || event === "stream") {
        if (data?.asset && typeof data?.price === "number") {
          onTick({ symbol: data.asset, ts: Date.now(), price: data.price });
        }
      }
    } catch { /* ignore non-JSON frames */ }
  });

  ws.on("close", () => {
    console.warn("[quotex] disconnected — reconnecting in 5s");
    setTimeout(connectQuotex, 5000);
  });
  ws.on("error", (e) => console.error("[quotex] error", e.message));
}

connectQuotex();
console.log("Quotex collector started. Streaming to:", INGEST_URL);
