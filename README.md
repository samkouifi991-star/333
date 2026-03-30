# Kalshi Edge Bot

Live dashboard + opportunity scanner for Kalshi markets.

## Deploy to Railway

1. Push this folder to a GitHub repo (or use Railway CLI)
2. Set environment variables:

| Variable | Required | Description |
|----------|----------|-------------|
| `API_KEY` | ✅ | Kalshi API Bearer token |
| `PORT` | ❌ | Auto-set by Railway |

3. Deploy — the dashboard is served at the root URL (`/`)

## Endpoints

| Route | Description |
|-------|-------------|
| `GET /` | Live dashboard UI |
| `GET /status` | Bot status JSON |
| `GET /opportunities` | Current opportunities JSON |

## Local Dev

```bash
cd kalshi-proxy
npm install
API_KEY=your_key node server.js
```

Open `http://localhost:3001`
