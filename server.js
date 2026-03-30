const express = require("express");
const { kalshiFetch, isConfigured, API_KEY_ID } = require("./src/kalshiClient");

const app = express();
const PORT = process.env.PORT || 8080;

const API_BASES = [
  "https://api.elections.kalshi.com",
];
let activeApiBase = API_BASES[0];

// ── In-memory state ───────────────────────────────────────────────────
let marketsCache = [];
let priceHistory = {};
let opportunities = [];
let lastFetchAt = null;
let fetchError = null;
let tickCount = 0;
const startedAt = Date.now();

// ── CORS ──────────────────────────────────────────────────────────────
app.use((_req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  if (_req.method === "OPTIONS") return res.sendStatus(200);
  next();
});
app.use(express.json());

// ── Health ────────────────────────────────────────────────────────────
app.get("/api/health", (_req, res) => {
  res.json({
    status: "ok",
    version: "7.0.0",
    authMethod: "RSA-PSS",
    uptime: Math.floor((Date.now() - startedAt) / 1000),
    tickCount,
    marketsCount: marketsCache.length,
    opportunitiesCount: opportunities.length,
    lastFetchAt,
    error: fetchError,
    apiKeySet: !!API_KEY_ID,
    privateKeyLoaded: isConfigured(),
    apiBase: activeApiBase,
  });
});

// ── Fetch markets page (authenticated) ────────────────────────────────
async function fetchMarketsPage(apiBase, cursor) {
  const params = new URLSearchParams({ limit: "200", status: "open" });
  if (cursor) params.set("cursor", cursor);

  return kalshiFetch(apiBase, `/markets?${params}`, "GET");
}

// ── Fetch all open markets ────────────────────────────────────────────
async function fetchAllMarkets() {
  let lastError = null;

  for (const apiBase of API_BASES) {
    const all = [];
    let cursor = null;
    let pages = 0;

    try {
      while (pages < 10) {
        const data = await fetchMarketsPage(apiBase, cursor);
        const markets = data.markets || [];
        all.push(...markets);
        cursor = data.cursor || null;
        if (!cursor || markets.length === 0) break;
        pages++;
      }

      if (all.length > 0) {
        activeApiBase = apiBase;
        console.log(`[INGEST] ${all.length} markets from ${apiBase} (${pages + 1} pages)`);
        return all;
      }
      lastError = new Error(`0 markets from ${apiBase}`);
    } catch (err) {
      console.error(`[INGEST] ${apiBase} failed: ${err.message}`);
      lastError = err;
    }
  }

  throw lastError || new Error("All API bases failed");
}

// ── Sport filter (category-based) ─────────────────────────────────────
function isSportsMarket(market) {
  if (!market) return false;
  const category = (
    market.category ||
    market.event_category ||
    market.series ||
    market.event_ticker ||
    ""
  ).toLowerCase();
  return category.includes("sports") || category.includes("sportsmultigameextended");
}

// ── Normalize market data ─────────────────────────────────────────────
function normalizeMarket(m) {
  if (!m || !m.ticker) return null;

  const price = m.yes_ask ?? m.last_price ?? null;
  if (price === null) return null;

  const ticker = m.ticker;
  const title = m.title || ticker;

  let sport = "other";
  const tickerUpper = ticker.toUpperCase();

  if (tickerUpper.startsWith("TENIS") || tickerUpper.startsWith("TENNIS") || (tickerUpper.includes("MATCH_WINNER") && tickerUpper.includes("TEN"))) sport = "tennis";
  else if (tickerUpper.startsWith("NBA")) sport = "nba";
  else if (tickerUpper.startsWith("NFL")) sport = "nfl";
  else if (tickerUpper.startsWith("NHL")) sport = "nhl";

  const hist = priceHistory[ticker] || [];
  const previousPrice = hist.length > 0 ? hist[hist.length - 1].price : price;

  return {
    ticker, title, sport,
    player1: m.subtitle || "", player2: "",
    lastPrice: price, previousPrice,
    yesBid: m.yes_bid ?? null, yesAsk: m.yes_ask ?? null,
    spread: (m.yes_ask != null && m.yes_bid != null) ? (m.yes_ask - m.yes_bid) : 99,
    volume: m.volume ?? 0,
    status: m.status === "open" ? "active" : m.status,
  };
}

// ── Regime Detection ──────────────────────────────────────────────────
function detectRegime(ticker) {
  const hist = priceHistory[ticker] || [];
  if (hist.length < 3) return { regime: "unknown", dropFromPeak: 0, ticksSinceNewLow: 0, peakPrice: 0 };

  const prices = hist.map(h => h.price);
  const peakPrice = Math.max(...prices);
  const current = prices[prices.length - 1];
  const dropFromPeak = peakPrice - current;

  let ticksSinceNewLow = 0;
  let runningLow = prices[prices.length - 1];
  for (let i = prices.length - 2; i >= 0; i--) {
    if (prices[i] <= runningLow) { runningLow = prices[i]; break; }
    ticksSinceNewLow++;
  }

  const prev = prices[prices.length - 2] || current;
  const uptick = current - prev;

  let regime = "normal";
  if (dropFromPeak >= 15) {
    if (ticksSinceNewLow >= 2 || uptick >= 1) regime = uptick >= 1 ? "recovery" : "stabilization";
    else regime = "panic";
  }

  return { regime, dropFromPeak, ticksSinceNewLow, peakPrice, uptick, recentLow: runningLow };
}

// ── Spike Detection ───────────────────────────────────────────────────
function detectSpike(ticker) {
  const hist = priceHistory[ticker] || [];
  if (hist.length < 3) return null;

  const now = Date.now();
  const recent = hist.filter(h => now - h.timestamp <= 120000);
  if (recent.length < 2) return null;

  const first = recent[0].price;
  const last = recent[recent.length - 1].price;
  const move = Math.abs(last - first);

  if (move >= 15) {
    const elapsed = (recent[recent.length - 1].timestamp - recent[0].timestamp) / 1000;
    return { move, direction: last < first ? "drop" : "surge", elapsed, speed: elapsed > 0 ? move / elapsed : move };
  }
  return null;
}

// ── Score opportunity ─────────────────────────────────────────────────
function scoreOpportunity(market, regimeData, spike) {
  let score = 0;
  score += Math.min((regimeData.dropFromPeak || 0) / 35, 1) * 35;
  if (regimeData.regime === "recovery") score += 20;
  else if (regimeData.regime === "stabilization") score += 12;
  score += Math.max(0, (3 - (market.spread || 99)) / 3) * 15;
  score += Math.min(Math.log10(Math.max(market.volume, 1)) / 4, 1) * 15;
  if (spike && spike.speed > 0.2) score += Math.min(spike.speed / 1, 1) * 15;
  return Math.round(score);
}

// ── Find opportunities ────────────────────────────────────────────────
function findOpportunities(markets) {
  const results = [];
  for (const m of markets) {
    if (m.status !== "active" || m.spread > 3 || m.volume < 100 || m.lastPrice < 20 || m.lastPrice > 55) continue;
    const regimeData = detectRegime(m.ticker);
    if (regimeData.dropFromPeak < 20) continue;
    if (regimeData.regime !== "stabilization" && regimeData.regime !== "recovery") continue;

    const spike = detectSpike(m.ticker);
    const score = scoreOpportunity(m, regimeData, spike);
    const type = (spike && spike.move >= 15 && spike.elapsed <= 120) ? "SPIKE" : "COMEBACK";
    const rebound = score >= 60 ? 15 : score >= 40 ? 12 : 8;

    results.push({
      ticker: m.ticker, title: m.title, sport: m.sport, player1: m.player1,
      entryPrice: m.lastPrice, peakPrice: regimeData.peakPrice, drop: regimeData.dropFromPeak,
      expectedRebound: rebound, targetPrice: m.lastPrice + rebound,
      spread: m.spread, volume: m.volume, regime: regimeData.regime,
      type, strength: score, spike: spike || null,
      reason: `${type}: dropped ${regimeData.dropFromPeak}¢ from ${regimeData.peakPrice}→${m.lastPrice}, ${regimeData.regime}, spread ${m.spread}¢, vol ${m.volume}`,
      timestamp: Date.now(),
    });
  }
  results.sort((a, b) => b.strength - a.strength);
  return results;
}

// ── In-memory raw ticker cache for debug ──────────────────────────────
let allTickersCache = [];
let tickerPrefixes = {};
let categoryBreakdown = {};

// ── Background tick ───────────────────────────────────────────────────
async function tick() {
  tickCount++;
  try {
    const raw = await fetchAllMarkets();

    // Build debug info: unique prefixes and categories
    const prefixes = {};
    const categories = {};
    for (const r of raw) {
      if (!r || !r.ticker) continue;
      const prefix = r.ticker.split("-")[0].toUpperCase();
      prefixes[prefix] = (prefixes[prefix] || 0) + 1;
      const cat = (r.category || r.event_ticker?.split("-")[0] || "unknown").toLowerCase();
      categories[cat] = (categories[cat] || 0) + 1;
    }
    tickerPrefixes = prefixes;
    categoryBreakdown = categories;
    allTickersCache = raw.filter(r => r && r.ticker).map(r => r.ticker).slice(0, 500);

    // TEMPORARY: log full first market object to find correct category field
    if (raw.length > 0) {
      console.log("[FULL MARKET SAMPLE]", JSON.stringify(raw[0], null, 2));
      // Log all unique keys that contain "cat", "series", "event", "tag"
      const sampleKeys = Object.keys(raw[0]).filter(k => /cat|series|event|tag|group/i.test(k));
      console.log("[CATEGORY-RELATED KEYS]", sampleKeys);
    }

    // Log top prefixes for debugging
    const topPrefixes = Object.entries(prefixes).sort((a, b) => b[1] - a[1]).slice(0, 20);
    console.log(`[DEBUG] Top ticker prefixes:`, topPrefixes);
    console.log(`[DEBUG] Categories:`, categories);

    const sportsOnly = raw.filter(r => {
      try { return r && r.ticker && isSportsMarket(r); } catch (e) { return false; }
    });
    console.log(`[FILTER] Sports markets: ${sportsOnly.length} / ${raw.length} total`);
    console.log(`[FILTER SAMPLE]`, sportsOnly.slice(0, 10).map(m => m.ticker));
    const normalized = [];
    for (const r of sportsOnly) {
      try {
        const m = normalizeMarket(r);
        if (m) normalized.push(m);
      } catch (err) {
        console.error("[MARKET ERROR]", r?.ticker, err.message);
      }
    }

    const now = Date.now();
    for (const m of normalized) {
      if (!priceHistory[m.ticker]) priceHistory[m.ticker] = [];
      priceHistory[m.ticker].push({ price: m.lastPrice, timestamp: now });
      if (priceHistory[m.ticker].length > 120) priceHistory[m.ticker].shift();
    }

    marketsCache = normalized;
    opportunities = findOpportunities(normalized);
    lastFetchAt = new Date().toISOString();
    fetchError = null;

    console.log(`[${lastFetchAt}] Tick #${tickCount}: ${normalized.length} markets, ${opportunities.length} opps`);
  } catch (err) {
    fetchError = err.message;
    console.error(`[TICK ERROR #${tickCount}] ${err.message}`);
  }
}

// ── Start polling ─────────────────────────────────────────────────────
if (isConfigured()) {
  tick();
  setInterval(tick, 7000);
  console.log("[BOT] Market fetcher started (RSA-PSS auth, 7s interval)");
} else {
  console.warn("[BOT] Auth not configured. Need KALSHI_API_KEY env + kalshi_private_key.pem file.");
}

// ── API Routes ────────────────────────────────────────────────────────
app.get("/api/tennis-markets", (_req, res) => {
  const limit = parseInt(_req.query.limit) || 200;
  const markets = marketsCache.filter(m => m.status === "active").sort((a, b) => b.volume - a.volume).slice(0, limit);
  res.json({ markets, count: markets.length, fetchedAt: lastFetchAt || new Date().toISOString() });
});

app.get("/api/opportunities", (_req, res) => {
  res.json({ opportunities, count: opportunities.length, fetchedAt: lastFetchAt, tickCount });
});

app.get("/status", (_req, res) => {
  res.json({
    running: isConfigured(), version: "7.6.0", authMethod: "RSA-PSS",
    tickCount, totalMarkets: marketsCache.length, opportunitiesFound: opportunities.length,
    lastFetchAt, error: fetchError, apiBase: activeApiBase,
    uptime: Math.floor((Date.now() - startedAt) / 1000),
  });
});

// ── Debug: see what tickers Kalshi actually returns ───────────────────
app.get("/api/debug/tickers", (_req, res) => {
  const search = (_req.query.search || "").toUpperCase();
  let tickers = allTickersCache;
  if (search) tickers = tickers.filter(t => t.toUpperCase().includes(search));
  res.json({
    totalMarkets: allTickersCache.length,
    tickerPrefixes,
    categoryBreakdown,
    matchingTickers: tickers.slice(0, 100),
    matchCount: tickers.length,
    sportsFilteredCount: marketsCache.length,
  });
});

// ── Start ─────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`Kalshi Edge Bot v7 (RSA-PSS) running on port ${PORT}`);
  console.log(`Auth configured: ${isConfigured() ? "Yes" : "No"}`);
});
