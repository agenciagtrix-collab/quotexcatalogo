# Quotex Collector Worker

Worker Node.js que se conecta à Quotex, escuta velas em tempo real e envia
para a plataforma via `POST /api/public/ingest` com assinatura HMAC-SHA256.

**Rode FORA do Lovable** (Railway, Render, Fly.io, VPS). O runtime serverless
da plataforma não suporta conexões WebSocket persistentes nem Playwright.

## Variáveis de ambiente

```
INGEST_URL=https://<seu-projeto>.lovable.app/api/public/ingest
INGEST_HMAC_SECRET=<mesmo valor configurado no Lovable Cloud>
QUOTEX_SSID=<seu SSID extraído do navegador>
QUOTEX_ASSETS=EURUSD-OTC,GBPUSD-OTC,USDJPY-OTC,AUDCAD-OTC
```

### Como obter o SSID

1. Faça login em https://qxbroker.com no navegador
2. Abra DevTools → Network → filtre por "ws"
3. Encontre a conexão `wss://ws2.qxbroker.com/socket.io/...`
4. Nas primeiras mensagens trocadas, procure por uma string `42["authorization",{"session":"..."}]`
5. O valor de `session` é o seu SSID

> Aviso: o SSID expira (~30 dias). Implementação production-ready deve usar
> Playwright para re-logar e renovar automaticamente.

## Deploy no Railway (recomendado)

```bash
cd worker
npm install
# configure as 4 envs acima no painel Railway
npm start
```

## Estrutura

- `index.js` — entrypoint: conecta WS, recebe velas, faz batch e POST
- `quotex.js` — cliente Socket.IO da Quotex
- `sign.js` — gera o header `x-signature`

## Como funciona o protocolo Quotex

Quotex usa Socket.IO sobre WS. Eventos principais:
- `authorization` — autentica com SSID
- `instruments/list` — lista de ativos disponíveis
- `instruments/follow` — subscreve a um ativo
- `candles/load` — recebe histórico
- O servidor envia ticks/velas via eventos não nomeados (frame `42[...]`)

Como o protocolo muda periodicamente, o coletor precisa de manutenção
quando a Quotex altera nomes de eventos ou estrutura de payload.
