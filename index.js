// Quotex collector worker — Fase 1 (instrumentado para diagnóstico).
// - Loga frames brutos nos primeiros 60s após cada conexão
// - Handshake Socket.IO v3 correto (espera "0", responde "40", depois "42[...]")
// - Headers de browser real no upgrade
// - Watchdog: força close se nenhum frame chegar em 15s
// - Logs detalhados de error/close

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

// ---------------------------------------------------------------- ingest
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
    if (!res.ok) BATCH.unshift(...candles);
  } catch (e) {
    console.error("[ingest] error", e.message);
    BATCH.unshift(...candles);
  }
}
setInterval(flush, FLUSH_MS);

// ---------------------------------------------------------------- candles
const buckets = new Map();
function onTick({ symbol, ts, price }) {
  for (const tf of [1, 5, 15]) {
    const bucketMs = tf * 60_000;
    const bucketTs = Math.floor(ts / bucketMs) * bucketMs;
    const key = `${symbol}:${tf}:${bucketTs}`;
    let b = buckets.get(key);
    if (!b) {
      b = {
        open: price, high: price, low: price, close: price,
        opened_at: new Date(bucketTs).toISOString(),
        symbol, timeframe: tf, _bucketTs: bucketTs,
      };
      buckets.set(key, b);
    } else {
      b.high = Math.max(b.high, price);
      b.low = Math.min(b.low, price);
      b.close = price;
    }
  }
  const now = Date.now();
  for (const [k, b] of buckets) {
    if (b._bucketTs + b.timeframe * 60_000 <= now) {
      BATCH.push({
        symbol: b.symbol, timeframe: b.timeframe, opened_at: b.opened_at,
        open: b.open, high: b.high, low: b.low, close: b.close,
      });
      buckets.delete(k);
    }
  }
}

// ---------------------------------------------------------------- quotex WS
function connectQuotex() {
  const url = "wss://ws2.qxbroker.com/socket.io/?EIO=3&transport=websocket";
  console.log(`[quotex] connecting to ${url}`);

  const ws = new WebSocket(url, {
    headers: {
      Origin: "https://qxbroker.com",
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
      "Accept-Language": "en-US,en;q=0.9",
      "Cache-Control": "no-cache",
      Pragma: "no-cache",
    },
  });

  const connectedAt = Date.now();
  let framesReceived = 0;
  let authSent = false;

  // Watchdog: 15s sem nenhum frame = mata
  const watchdog = setTimeout(() => {
    if (framesReceived === 0) {
      console.error(
        "[quotex] silent connection (0 frames in 15s) — possible auth/IP block",
      );
      try { ws.terminate(); } catch {}
    }
  }, 15_000);

  ws.on("open", () => {
    console.log("[quotex] socket open — waiting for handshake frame");
  });

  ws.on("message", (raw) => {
    const text = raw.toString();
    framesReceived++;

    // Log dos primeiros 60s para diagnóstico
    if (Date.now() - connectedAt < 60_000) {
      console.log(`[quotex<-] ${text.slice(0, 300)}`);
    }

    // Socket.IO engine ping
    if (text === "2") { ws.send("3"); return; }
    if (text === "3") return;

    // Frame "0{...}" = handshake do engine.io aberto → mandar "40" (connect namespace)
    if (text.startsWith("0{")) {
      console.log("[quotex->] 40 (namespace connect)");
      ws.send("40");
      return;
    }

    // Frame "40" = namespace conectado → autenticar
    if (text === "40" || text.startsWith("40{")) {
      if (authSent) return;
      authSent = true;
      const authFrame =
        `42["authorization",${JSON.stringify({
          session: QUOTEX_SSID, isDemo: 0, tournamentId: 0,
        })}]`;
      console.log("[quotex->] authorization");
      ws.send(authFrame);
      // subscrever ativos
      for (const symbol of ASSETS) {
        const sub =
          `42["instruments/follow",${JSON.stringify({ asset: symbol, period: 60 })}]`;
        ws.send(sub);
      }
      console.log(`[quotex->] subscribed to ${ASSETS.length} assets`);
      return;
    }

    // Erro de connect / disconnect do namespace
    if (text.startsWith("44") || text.startsWith("41")) {
      console.error(`[quotex] server rejected: ${text.slice(0, 300)}`);
      return;
    }

    // Eventos de dados: "42[event, payload]"
    if (text.startsWith("42")) {
      try {
        const payload = JSON.parse(text.slice(2));
        const [event, data] = payload;
        if (
          (event === "instruments/update" || event === "stream") &&
          data?.asset && typeof data?.price === "number"
        ) {
          onTick({ symbol: data.asset, ts: Date.now(), price: data.price });
        }
      } catch { /* ignore */ }
    }
  });

  ws.on("error", (e) => {
    console.error(`[quotex] error: ${e.message}`);
  });

  ws.on("close", (code, reasonBuf) => {
    clearTimeout(watchdog);
    const reason = reasonBuf?.toString() || "(no reason)";
    const lifeMs = Date.now() - connectedAt;
    console.warn(
      `[quotex] closed code=${code} reason=${reason} frames=${framesReceived} lived=${lifeMs}ms — reconnecting in 5s`,
    );
    setTimeout(connectQuotex, 5000);
  });
}

connectQuotex();
console.log("Quotex collector started. Streaming to:", INGEST_URL);
