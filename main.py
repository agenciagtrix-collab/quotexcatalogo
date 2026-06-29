"""
Quotex collector worker — Fase 4.

Modos:
  python main.py            → live polling (default)
  python main.py --backfill → backfill últimos 7 dias e sai

Novidades Fase 4:
- Lista de ativos é refrescada dinamicamente a partir de
  {ACTIVE_ASSETS_URL} (default: derivado de INGEST_URL → /active-assets)
  a cada ASSETS_REFRESH_SECONDS (default 60s). Mudanças em /settings no
  dashboard propagam sem redeploy.
- QUOTEX_ASSETS continua sendo o fallback inicial caso o endpoint falhe.
"""

from __future__ import annotations

import argparse
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


# ---------------------------------------------------------------- logging
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        extra = getattr(record, "extra", None)
        if isinstance(extra, dict):
            payload.update(extra)
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler], force=True)
log = logging.getLogger("collector")


def logj(level: int, msg: str, **fields: Any) -> None:
    log.log(level, msg, extra={"extra": fields})


# ---------------------------------------------------------------- env
def env(name: str, default: str | None = None, required: bool = True) -> str:
    v = os.environ.get(name, default)
    if required and not v:
        logj(logging.ERROR, "missing_env", var=name)
        sys.exit(1)
    return v  # type: ignore[return-value]


INGEST_URL = env("INGEST_URL")
_BASE = INGEST_URL.rsplit("/", 1)[0]
HEARTBEAT_URL = env("HEARTBEAT_URL", _BASE + "/heartbeat", required=False)
ACTIVE_ASSETS_URL = env("ACTIVE_ASSETS_URL", _BASE + "/active-assets", required=False)
INGEST_HMAC_SECRET = env("INGEST_HMAC_SECRET")
QUOTEX_EMAIL = env("QUOTEX_EMAIL")
QUOTEX_PASSWORD = env("QUOTEX_PASSWORD")
ASSETS: list[str] = [
    a.strip() for a in env("QUOTEX_ASSETS", "", required=False).split(",") if a.strip()
]
TIMEFRAMES = [
    int(t.strip())
    for t in env("QUOTEX_TIMEFRAMES", "60,300,900", required=False).split(",")
    if t.strip()
]
ACCOUNT = env("QUOTEX_ACCOUNT", "demo", required=False).lower()
WORKER_ID = env("WORKER_ID", "quotex-railway", required=False)
BACKFILL_DAYS = int(env("BACKFILL_DAYS", "7", required=False))
ASSETS_REFRESH_SECONDS = int(env("ASSETS_REFRESH_SECONDS", "60", required=False))


# ---------------------------------------------------------------- http
def sign_body(body: str) -> str:
    return hmac.new(INGEST_HMAC_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()


async def post_candles(client: httpx.AsyncClient, candles: list[dict[str, Any]]) -> dict[str, Any]:
    if not candles:
        return {"inserted": 0}
    body = json.dumps({"candles": candles}, separators=(",", ":"))
    sig = sign_body(body)
    try:
        r = await client.post(
            INGEST_URL,
            content=body,
            headers={"content-type": "application/json", "x-signature": sig},
            timeout=30,
        )
        try:
            payload = r.json()
        except Exception:
            payload = {"raw": r.text[:200]}
        logj(
            logging.INFO if r.status_code == 200 else logging.WARNING,
            "ingest_response",
            status=r.status_code, batch=len(candles), payload=payload,
        )
        return {"status": r.status_code, **(payload if isinstance(payload, dict) else {})}
    except Exception as e:  # noqa: BLE001
        logj(logging.ERROR, "ingest_error", error=str(e))
        return {"error": str(e)}


_total_sent = 0

async def heartbeat(client: httpx.AsyncClient, status: str, last_batch: int, last_error: str | None) -> None:
    body = json.dumps({
        "worker_id": WORKER_ID,
        "status": status,
        "candles_last_batch": last_batch,
        "candles_total": _total_sent,
        "last_error": last_error,
        "meta": {"assets": ASSETS, "timeframes": TIMEFRAMES, "account": ACCOUNT},
    }, separators=(",", ":"))
    sig = sign_body(body)
    try:
        r = await client.post(
            HEARTBEAT_URL,
            content=body,
            headers={"content-type": "application/json", "x-signature": sig},
            timeout=10,
        )
        if r.status_code != 200:
            logj(logging.WARNING, "heartbeat_failed", status=r.status_code, body=r.text[:200])
    except Exception as e:  # noqa: BLE001
        logj(logging.WARNING, "heartbeat_error", error=str(e))


# ---------------------------------------------------------------- dynamic assets
_last_assets_refresh: float = 0.0

async def refresh_assets(client: httpx.AsyncClient, force: bool = False) -> None:
    """Refresh ASSETS list from the platform every ASSETS_REFRESH_SECONDS."""
    global _last_assets_refresh, ASSETS
    now = time.time()
    if not force and (now - _last_assets_refresh) < ASSETS_REFRESH_SECONDS:
        return
    _last_assets_refresh = now
    try:
        r = await client.get(ACTIVE_ASSETS_URL, timeout=10)
        if r.status_code != 200:
            logj(logging.WARNING, "active_assets_http", status=r.status_code, body=r.text[:200])
            return
        data = r.json()
        new_assets = [a for a in (data.get("assets") or []) if isinstance(a, str) and a]
        if not new_assets:
            logj(logging.WARNING, "active_assets_empty")
            return
        if set(new_assets) != set(ASSETS):
            logj(logging.INFO, "active_assets_refreshed", before=ASSETS, after=new_assets)
            ASSETS = new_assets
        else:
            logj(logging.INFO, "active_assets_unchanged", count=len(ASSETS))
    except Exception as e:  # noqa: BLE001
        logj(logging.WARNING, "active_assets_error", error=str(e))


# ---------------------------------------------------------------- candles
_last_sent: dict[tuple[str, int], int] = {}


def normalize(asset: str, tf: int, raw: dict[str, Any]) -> dict[str, Any] | None:
    try:
        ts = int(raw.get("time") or raw.get("t") or 0)
        o = float(raw["open"]); h = float(raw["high"])
        low = float(raw["low"]); c = float(raw["close"])
    except (KeyError, TypeError, ValueError):
        return None
    if ts <= 0:
        return None
    return {
        "symbol": asset,
        "timeframe": tf // 60,
        "opened_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        "open": o, "high": h, "low": low, "close": c,
    }


async def poll_once(qx: Quotex, http: httpx.AsyncClient) -> tuple[int, str | None]:
    global _total_sent
    now = time.time()
    batch: list[dict[str, Any]] = []
    last_err: str | None = None
    for asset in ASSETS:
        for tf in TIMEFRAMES:
            offset = tf * 5
            try:
                candles = await qx.get_candles(asset, now, offset, tf)
            except Exception as e:  # noqa: BLE001
                last_err = f"{asset}/{tf}: {e}"
                logj(logging.WARNING, "get_candles_failed", asset=asset, tf=tf, error=str(e))
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
        result = await post_candles(http, batch)
        if isinstance(result.get("status"), int) and result["status"] == 200:
            _total_sent += int(result.get("inserted", 0))
        else:
            last_err = f"ingest: {result}"
    return len(batch), last_err


# ---------------------------------------------------------------- backfill
async def backfill(qx: Quotex, http: httpx.AsyncClient) -> None:
    end_ts = time.time()
    seconds_total = BACKFILL_DAYS * 86400
    logj(logging.INFO, "backfill_start", days=BACKFILL_DAYS, assets=ASSETS, timeframes=TIMEFRAMES)

    for asset in ASSETS:
        for tf in TIMEFRAMES:
            collected = 0
            cursor = end_ts
            target = end_ts - seconds_total
            while cursor > target:
                window = tf * 60
                try:
                    candles = await qx.get_candles(asset, cursor, window, tf)
                except Exception as e:  # noqa: BLE001
                    logj(logging.WARNING, "backfill_chunk_failed", asset=asset, tf=tf, cursor=cursor, error=str(e))
                    break
                if not candles:
                    break
                normalized = [n for raw in candles if (n := normalize(asset, tf, raw))]
                for i in range(0, len(normalized), 200):
                    await post_candles(http, normalized[i:i + 200])
                collected += len(normalized)
                oldest = min((int(c.get("time") or 0) for c in candles), default=0)
                if oldest <= 0 or oldest >= int(cursor):
                    break
                cursor = oldest - 1
                await asyncio.sleep(0.2)
            logj(logging.INFO, "backfill_asset_done", asset=asset, tf=tf, candles=collected)
    logj(logging.INFO, "backfill_done")


# ---------------------------------------------------------------- main
async def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true", help="Run historical backfill and exit")
    args = parser.parse_args()

    logj(logging.INFO, "starting", worker_id=WORKER_ID, assets=ASSETS, timeframes=TIMEFRAMES, account=ACCOUNT, mode="backfill" if args.backfill else "live")

    qx = Quotex(email=QUOTEX_EMAIL, password=QUOTEX_PASSWORD, lang="pt")
    ok, msg = await qx.connect()
    if not ok:
        logj(logging.ERROR, "quotex_connect_failed", message=str(msg))
        sys.exit(2)
    logj(logging.INFO, "quotex_connected", message=str(msg))

    await qx.change_account("REAL" if ACCOUNT == "real" else "PRACTICE")
    try:
        bal = await qx.get_balance()
        logj(logging.INFO, "quotex_balance", balance=bal, account=ACCOUNT)
    except Exception as e:  # noqa: BLE001
        logj(logging.WARNING, "quotex_balance_failed", error=str(e))

    async with httpx.AsyncClient() as http:
        # carrega lista dinâmica antes de qualquer coleta
        await refresh_assets(http, force=True)
        if not ASSETS:
            logj(logging.ERROR, "no_assets_available")
            sys.exit(3)

        if args.backfill:
            await backfill(qx, http)
            await heartbeat(http, "ok", 0, None)
            return

        interval = max(15, min(min(TIMEFRAMES) // 4, 60))
        logj(logging.INFO, "poll_loop", interval_seconds=interval)
        await heartbeat(http, "ok", 0, None)
        while True:
            try:
                await refresh_assets(http)
                count, err = await poll_once(qx, http)
                await heartbeat(http, "ok" if not err else "degraded", count, err)
            except Exception as e:  # noqa: BLE001
                logj(logging.ERROR, "poll_loop_error", error=str(e))
                await heartbeat(http, "error", 0, str(e))
            await asyncio.sleep(interval)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
