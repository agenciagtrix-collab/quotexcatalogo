# Quotex Collector Worker — Fase 2 (Python + pyquotex)

Worker Python que se conecta à Quotex usando a biblioteca
[`pyquotex`](https://github.com/cleitonleonel/pyquotex) (login real com
e-mail/senha, mantém sessão e reconecta sozinha), faz polling das velas
fechadas e envia para o backend via `POST /api/public/ingest` assinado
com HMAC-SHA256.

> Rode FORA do Lovable (Railway, Render, Fly.io, VPS). O runtime
> serverless da plataforma não suporta conexões WebSocket persistentes.

## Por que abandonamos o worker em Node.js?

A Fase 1 (Node + `ws`) chegou até autenticar via SSID, mas a Quotex usa
frames binários proprietários para entregar os ticks. Reimplementar isso
em Node toma semanas e quebra a cada update da corretora. `pyquotex` já
implementa o protocolo inteiro e é mantido pela comunidade.

## Variáveis de ambiente

| Nome | Obrigatória | Default | Descrição |
|------|------|------|------|
| `INGEST_URL` | ✅ | https://quotexnew.lovable.app/api/public/ingest | `https://<seu-projeto>.lovable.app/api/public/ingest` |
| `INGEST_HMAC_SECRET` | ✅ | SF2FnkuauYe9y1V5X7YbPP3Yrq1kzKTNzuOyvIkNnTL | Mesmo valor configurado no Lovable Cloud |
| `QUOTEX_EMAIL` | ✅ | — | novastrends@gmail.com |
| `QUOTEX_PASSWORD` | ✅ | — | #Meunegocio123 |
| `QUOTEX_ASSETS` | ✅ | — | CSV, ex.: `EURUSD_otc,GBPUSD_otc,USDJPY_otc` |
| `QUOTEX_TIMEFRAMES` | ⬜ | `60,300,900` | Segundos (1m, 5m, 15m) |
| `QUOTEX_ACCOUNT` | ⬜ | `demo` | `demo` ou `real` |

> Os nomes dos ativos seguem o padrão `pyquotex` (snake_case + `_otc`),
> não o do MetaTrader. Use `EURUSD_otc`, não `EURUSD-OTC`.

## Deploy no Railway

1. **Settings → Source**
   - Repo: o mesmo do projeto
   - Branch: `main`
   - **Root Directory: `worker`** (importante!)
2. **Settings → Build**
   - Builder: `Dockerfile` (Railway detecta automático)
3. **Variables** — cole as 5 obrigatórias acima.
4. **Deploy** — primeiro build leva ~3-4 min (instala pyquotex via git).

### Logs esperados

```
Quotex collector starting | assets=['EURUSD_otc'] | ...
[quotex] connected (...)
[quotex] balance=352.99 account=demo
[poll] interval=15s
[poll] 3 new candles to ingest
[ingest] 200 ok (3 candles)
```

Se aparecer `[ingest] 401` → `INGEST_HMAC_SECRET` está diferente do que
está salvo no Lovable Cloud. Atualize a env no Railway.

## Como o protocolo funciona agora

`pyquotex` cuida de tudo:
- Login (email/senha) → obtém SSID válido
- Mantém WebSocket aberto, faz ping/pong, reconecta
- Decodifica frames binários e devolve candles em dict Python

O nosso worker só precisa fazer polling de `get_candles()` por ativo +
timeframe, deduplicar pela timestamp, e empurrar pro `/api/public/ingest`.
