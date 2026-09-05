# STRIX — Solana Research + Paper Execution Desk

Production-oriented multi-agent Solana research framework. **Paper trading only:** this repository contains no wallet secret loading, signing, transaction submission, or live-money execution path.

## Pipeline

`Solana/DEX ingestion → security → wallet intelligence → market structure → multi-agent consensus → risk → paper execution → audit ledger → dashboard`

## What is implemented

- React/TypeScript live dashboard, deployable to Vercel.
- FastAPI/asyncio engine with WebSocket telemetry.
- DEX Screener Solana token-profile discovery + pair enrichment.
- Solana RPC mint/freeze authority and top-holder checks.
- Pump program log subscription for read-only discovery.
- Wallet reputation table for rug-history feedback.
- Security, momentum, wallet-cluster, and risk/execution agent contracts with structured outputs.
- Paper entries, stops, take-profit, trailing-stop logic, slippage approximation, mark-to-market P&L.
- SQLite locally or PostgreSQL via `STRIX_DATABASE_URL`.
- Audit ledger for candidates, agent results, decisions, entries, and exits.
- Demo mode for UI verification without presenting demo events as live market results.

## Architecture note

The Vercel project hosts the dashboard. The scanner is an always-on Python process and should run in a persistent worker environment. Set `VITE_STRIX_ENGINE_HTTP` and `VITE_STRIX_ENGINE_WS` in Vercel to the public engine endpoint.

## Local run

```bash
cp .env.example .env
python -m venv .venv
. .venv/bin/activate
pip install -r engine/requirements.txt
PYTHONPATH=engine uvicorn strix_engine.api:app --host 0.0.0.0 --port 8000
```

In another shell:

```bash
npm install
npm run dev
```

For UI-only verification, set `STRIX_DEMO_MODE=true` on the Python engine. Demo decisions are labelled `DEMO` and are never presented as live trades.

## Security defaults

- Active mint authority → reject.
- Active freeze authority → reject.
- Top-10 holder concentration above 20% → reject.
- Missing holder concentration → reject (fail closed).
- Known deployer rug history → reject.
- DEX LP lock below configured threshold → reject when an LP-lock provider is present; missing LP proof is explicitly penalized and logged.
- Any agent veto overrides weighted consensus.
- No signing code exists in this repo.

## Next production integrations

1. Helius/QuickNode dedicated RPC + WebSocket endpoint.
2. Venue-specific LP lock/burn attestation provider.
3. Funded-by/deployer graph indexer for cluster scoring.
4. Unsigned buy/sell simulation provider; simulation failure must reject, never fall through to a paper BUY.
5. Optional LLM provider behind typed AgentResult JSON; deterministic risk gates remain the audit baseline.

## Vercel

Build preset is included in `vercel.json`. Required dashboard variables:

- `VITE_STRIX_ENGINE_HTTP=https://your-engine.example`
- `VITE_STRIX_ENGINE_WS=wss://your-engine.example/ws`
