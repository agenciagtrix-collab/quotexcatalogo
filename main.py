"""Quotex collector worker — Fase 2 (Python + pyquotex)."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from pyquotex.stable_api import Quotex

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("collector")


def env(name: str, default: str | None = None, required: bool = True) -> str:
    v = os.environ.get(name, default)
    if required and not v:
        log.error("Missing env %s", name)
        sys.exit(1)
    return v  # type: ignore[return-value]


INGEST_URL = env("INGEST_URL")
INGEST_HMAC_SECRET = env("INGEST_HMAC_SECRET")
QUOTEX_EMAIL = env("QUOTEX_EMAIL")
QUOTEX_PASSWORD = env("QUOTEX_PASSWORD")
ASSETS = [a.strip() for a in env("QUOTEX_ASSETS").split(",") if a.strip()]
TIMEFRAMES = [int(t) for t in env("QUOTEX_TIMEFRAMES", "60,300,900", required=False).split(",") if t.strip()]
ACCOUNT = env("QUOTEX_ACCOUNT", "demo", required=False).lower()


def sign_body(body: str) -> str:
    return hmac.new(INGEST_HMAC_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()


async def post_candles(client: httpx.AsyncClient, candles: list[dict[str, Any]]) -> None:
    if not candles:
        return
    body = json.dumps({"candles": candles}, separators=(",", ":"))
    try:
        r = await client.post(INGEST_URL, content=body,
            headers={"content-type": "application/json", "x-signature": sign_body(body)}, timeout=20)
        log.info("[ingest] %s %s (%d candles)", r.status_code, r.text[:200], len(candles))
    except Exception as e:
        log.error("[ingest] error: %s", e)


_last_sent: dict[tuple[str, int], int] = {}


def normalize(asset: str, tf: int, raw: dict[str, Any]) -> dict[str, Any] | None:
    try:
        ts = int(raw.get("time") or raw.get("t") or 0)
        return {
            "symbol": asset, "timeframe": tf // 60,
            "opened_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            "open": float(raw["open"]), "high": float(raw["high"]),
            "low": float(raw["low"]), "close": float(raw["close"]),
        } if ts > 0 else None
    except (KeyError, TypeError, ValueError):
        return None


async def poll_once(qx: Quotex, http: httpx.AsyncClient) -> None:
    now = time.time()
    batch: list[dict[str, Any]] = []
    for asset in ASSETS:
        for tf in TIMEFRAMES:
            try:
                candles = await qx.get_candles(asset, now, tf * 5, tf)
            except Exception as e:
                log.warning("[quotex] get_candles %s/%ds: %s", asset, tf, e)
                continue
            if not candles:
                continue
            for raw in candles[:-1]:
                ts = int(raw.get("time") or 0)
                if ts <= _last_sent.get((asset, tf), 0):
                    continue
                n = normalize(asset, tf, raw)
                if n:
                    batch.append(n)
                    _last_sent[(asset, tf)] = ts
    if batch:
        log.info("[poll] %d new candles to ingest", len(batch))
        await post_candles(http, batch)


async def main() -> None:
    log.info("Quotex collector starting | assets=%s | timeframes=%s | account=%s", ASSETS, TIMEFRAMES, ACCOUNT)
    qx = Quotex(email=QUOTEX_EMAIL, password=QUOTEX_PASSWORD, lang="pt")
    ok, msg = await qx.connect()
    if not ok:
        log.error("[quotex] connect failed: %s", msg)
        sys.exit(2)
    log.info("[quotex] connected (%s)", msg)

    await qx.change_account("REAL" if ACCOUNT == "real" else "PRACTICE")
    try:
        bal = await qx.get_balance()
        log.info("[quotex] balance=%s account=%s", bal, ACCOUNT)
    except Exception as e:
        log.warning("[quotex] balance read failed: %s", e)

    interval = max(15, min(min(TIMEFRAMES) // 4, 60))
    log.info("[poll] interval=%ds", interval)

    async with httpx.AsyncClient() as http:
        while True:
            try:
                await poll_once(qx, http)
            except Exception as e:
                log.exception("[poll] error: %s", e)
            await asyncio.sleep(interval)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
