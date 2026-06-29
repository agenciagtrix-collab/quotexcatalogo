"""
Quotex collector worker — Fase 2 (Python + pyquotex).

Conecta na Quotex via pyquotex (login email+senha, mantém SSID/session
internamente), faz polling de velas fechadas a cada timeframe e envia
em batch para POST {INGEST_URL} assinado com HMAC-SHA256.

Variáveis de ambiente:
  INGEST_URL            URL pública /api/public/ingest
  INGEST_HMAC_SECRET    Segredo compartilhado com o backend
  QUOTEX_EMAIL          E-mail da conta Quotex
  QUOTEX_PASSWORD       Senha da conta Quotex
  QUOTEX_ASSETS         Lista CSV, ex.: "EURUSD_otc,GBPUSD_otc"
  QUOTEX_TIMEFRAMES     CSV em segundos, default "60,300,900"
                        (=> 1m, 5m, 15m)
  QUOTEX_ACCOUNT        "demo" (default) ou "real"
"""

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
from pyquotex.utils.account_type import AccountType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
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
TIMEFRAMES = [
    int(t.strip())
    for t in env("QUOTEX_TIMEFRAMES", "60,300,900", required=False).split(",")
    if t.strip()
]
ACCOUNT = env("QUOTEX_ACCOUNT", "demo", required=False).lower()


# ---------------------------------------------------------------- ingest
def sign_body(body: str) -> str:
    return hmac.new(
        INGEST_HMAC_SECRET.encode(),
        body.encode(),
        hashlib.sha256,
    ).hexdigest()


async def post_candles(client: httpx.AsyncClient, candles: list[dict[str, Any]]) -> None:
    if not candles:
        return
    body = json.dumps({"candles": candles}, separators=(",", ":"))
    sig = sign_body(body)
    try:
        r = await client.post(
            INGEST_URL,
            content=body,
            headers={"content-type": "application/json", "x-signature": sig},
            timeout=20,
        )
        log.info("[ingest] %s %s (%d candles)", r.status_code, r.text[:200], len(candles))
    except Exception as e:  # noqa: BLE001
        log.error("[ingest] error: %s", e)


# ---------------------------------------------------------------- candles
# Memória do último candle enviado por (symbol, timeframe) — evita duplicar
_last_sent: dict[tuple[str, int], int] = {}


def normalize(asset: str, tf: int, raw: dict[str, Any]) -> dict[str, Any] | None:
    """Converte um candle da pyquotex pro formato do /api/public/ingest."""
    try:
        ts = int(raw.get("time") or raw.get("t") or 0)
        o = float(raw["open"])
        h = float(raw["high"])
        low = float(raw["low"])
        c = float(raw["close"])
    except (KeyError, TypeError, ValueError):
        return None
    if ts <= 0:
        return None
    return {
        "symbol": asset,
        "timeframe": tf // 60,  # API usa minutos
        "opened_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        "open": o,
        "high": h,
        "low": low,
        "close": c,
    }


async def poll_once(qx: Quotex, http: httpx.AsyncClient) -> None:
    now = time.time()
    batch: list[dict[str, Any]] = []
    for asset in ASSETS:
        for tf in TIMEFRAMES:
            offset = tf * 5  # 5 velas pra trás como margem
            try:
                candles = await qx.get_candles(asset, now, offset, tf)
            except Exception as e:  # noqa: BLE001
                log.warning("[quotex] get_candles %s/%ds: %s", asset, tf, e)
                continue
            if not candles:
                continue
            # Última vela COMPLETAMENTE fechada = penúltima do retorno
            for raw in candles[:-1]:
                ts = int(raw.get("time") or 0)
                # só envia se for nova
                if ts <= _last_sent.get((asset, tf), 0):
                    continue
                n = normalize(asset, tf, raw)
                if n:
                    batch.append(n)
                    _last_sent[(asset, tf)] = ts
    if batch:
        log.info("[poll] %d new candles to ingest", len(batch))
        await post_candles(http, batch)


async def connect_with_retry(max_retries: int = 10) -> Quotex:
    """Conecta na Quotex com retry exponencial."""
    qx = Quotex(email=QUOTEX_EMAIL, password=QUOTEX_PASSWORD, lang="pt")
    
    for attempt in range(1, max_retries + 1):
        try:
            ok, msg = await qx.connect()
            if ok:
                log.info("[quotex] connected (%s)", msg)
                return qx
            else:
                log.warning("[quotex] connect attempt %d failed: %s", attempt, msg)
        except Exception as e:  # noqa: BLE001
            log.warning("[quotex] connect attempt %d error: %s", attempt, e)
        
        if attempt < max_retries:
            # Backoff exponencial: 5s, 10s, 20s, 40s, etc (máx 120s)
            wait_time = min(5 * (2 ** (attempt - 1)), 120)
            log.info("[quotex] retrying in %ds (attempt %d/%d)", wait_time, attempt, max_retries)
            await asyncio.sleep(wait_time)
    
    log.error("[quotex] failed to connect after %d attempts", max_retries)
    sys.exit(2)


async def main() -> None:
    log.info(
        "Quotex collector starting | assets=%s | timeframes=%s | account=%s",
        ASSETS, TIMEFRAMES, ACCOUNT,
    )

    qx = await connect_with_retry()

    await qx.change_account(
        AccountType.REAL if ACCOUNT == "real" else AccountType.DEMO
    )
    try:
        bal = await qx.get_balance()
        log.info("[quotex] balance=%s account=%s", bal, ACCOUNT)
    except Exception as e:  # noqa: BLE001
        log.warning("[quotex] balance read failed: %s", e)

    # Intervalo: menor timeframe ÷ 4 (margem); mín 15s, máx 60s
    interval = max(15, min(min(TIMEFRAMES) // 4, 60))
    log.info("[poll] interval=%ds", interval)

    async with httpx.AsyncClient() as http:
        while True:
            try:
                await poll_once(qx, http)
            except Exception as e:  # noqa: BLE001
                log.exception("[poll] error: %s", e)
            await asyncio.sleep(interval)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

