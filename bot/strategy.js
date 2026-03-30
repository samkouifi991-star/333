/**
 * V4 Mean Reversion Strategy — Tennis Optimized
 * Entry only during panic → stabilization/recovery transitions.
 * Backtested config: SL -5¢ / TP +10¢ / Hold 5 games
 */

const STRATEGY_ID = "tennis-mean-reversion-v4";

const CONFIG = {
  // Entry filters
  preMatchMin: 45,
  preMatchMax: 55,
  entryZoneMin: 25,
  entryZoneMax: 35,
  minDropFromPeak: 20,       // ¢ drop required to trigger

  // Regime detection
  stabilizationTicks: 2,     // consecutive ticks without new low
  minUptickForRecovery: 1,   // ¢ upward move to confirm recovery

  // Exit rules
  takeProfitCents: 10,
  stopLossCents: 5,
  maxHoldGames: 5,
  maxPositionAgeMs: 300000,  // 5 min fallback

  // Spread / liquidity
  maxSpread: 3,              // tightened from 10 to 3
  minVolume: 5,

  // Position sizing
  defaultRiskPct: 1,         // % of bankroll per trade
  maxRiskPct: 3,
  scalingThreshold: 100,     // trades before scaling allowed

  // Risk management
  maxOpenTrades: 3,
  dailyStopLossPct: 5,       // % of bankroll
  globalDrawdownPct: 10,     // % of initial bankroll → full pause
};

/**
 * Score a signal for quality ranking.
 * Higher = better trade.
 */
function scoreSignal({ dropFromPeak, ticksSinceNewLow, regime, spread, volume }) {
  let score = 0;

  // Drop size (>20¢ stronger, max ~30¢)
  score += Math.min(dropFromPeak / 30, 1) * 35;

  // Stabilization quality
  score += Math.min(ticksSinceNewLow / 5, 1) * 20;

  // Recovery bonus
  if (regime === "recovery") score += 15;
  else if (regime === "stabilization") score += 8;

  // Spread tightness (lower = better, max 3¢)
  score += Math.max(0, (3 - spread) / 3) * 15;

  // Volume (log scale)
  score += Math.min(Math.log10(Math.max(volume, 1)) / 3, 1) * 15;

  return Math.round(score);
}

/**
 * Detect regime from tick history.
 */
function detectRegime(tickHistory) {
  if (!tickHistory || tickHistory.length < 3) {
    return { regime: "unknown", ticksSinceNewLow: 0, recentLow: 0, peakPrice: 0 };
  }

  const prices = tickHistory.map(t => t.price);
  const peakPrice = Math.max(...prices);
  const current = prices[prices.length - 1];
  const dropFromPeak = peakPrice - current;

  let ticksSinceNewLow = 0;
  let runningLow = prices[prices.length - 1];
  for (let i = prices.length - 2; i >= 0; i--) {
    if (prices[i] <= runningLow) {
      runningLow = prices[i];
      break;
    }
    ticksSinceNewLow++;
  }

  const prev = prices[prices.length - 2] || current;
  const uptick = current - prev;

  let regime = "normal";

  if (dropFromPeak >= CONFIG.minDropFromPeak) {
    if (ticksSinceNewLow >= CONFIG.stabilizationTicks || uptick >= CONFIG.minUptickForRecovery) {
      regime = uptick >= CONFIG.minUptickForRecovery ? "recovery" : "stabilization";
    } else {
      regime = "panic";
    }
  }

  return {
    regime,
    ticksSinceNewLow,
    recentLow: runningLow,
    peakPrice,
    dropFromPeak,
    uptick,
  };
}

/**
 * Evaluate markets for v4 entry signals.
 * Returns signals sorted by quality score — only top-ranked are actionable.
 */
function evaluate(markets, priceHistory, activePositions, riskState = {}) {
  const signals = [];
  const regimes = {};
  const now = Date.now();

  const openCount = riskState.openTradeCount || Object.keys(activePositions).length;
  if (openCount >= CONFIG.maxOpenTrades) {
    return { signals, regimes, config: CONFIG, blocked: "max_open_trades" };
  }

  const dailyLossLimit = (riskState.bankroll || 10000) * (CONFIG.dailyStopLossPct / 100);
  if ((riskState.dailyPnl || 0) <= -dailyLossLimit) {
    return { signals, regimes, config: CONFIG, blocked: "daily_stop_loss" };
  }

  // Global drawdown check
  if (riskState.globalPaused) {
    return { signals, regimes, config: CONFIG, blocked: "global_drawdown" };
  }

  for (const market of markets) {
    if (activePositions[market.ticker]) continue;

    // Spread filter (tightened to 3¢)
    if (market.spread > CONFIG.maxSpread) continue;
    if (market.volume < CONFIG.minVolume) continue;
    if (market.status !== "active") continue;

    const current = market.lastPrice;
    if (current < CONFIG.entryZoneMin || current > CONFIG.entryZoneMax) continue;

    const history = priceHistory[market.ticker] || [];
    const regimeData = detectRegime(history);
    regimes[market.ticker] = regimeData;

    if (regimeData.regime !== "stabilization" && regimeData.regime !== "recovery") continue;
    if (regimeData.dropFromPeak < CONFIG.minDropFromPeak) continue;
    if (regimeData.peakPrice < CONFIG.preMatchMin || regimeData.peakPrice > CONFIG.preMatchMax) continue;

    const quality = scoreSignal({
      dropFromPeak: regimeData.dropFromPeak,
      ticksSinceNewLow: regimeData.ticksSinceNewLow,
      regime: regimeData.regime,
      spread: market.spread,
      volume: market.volume,
    });

    const confidence = Math.min(quality / 100, 1);

    signals.push({
      strategyId: STRATEGY_ID,
      ticker: market.ticker,
      title: market.title,
      player1: market.player1,
      player2: market.player2,
      action: "BUY",
      side: "yes",
      price: current,
      limitPrice: market.yesAsk || current + 1,
      peakPrice: regimeData.peakPrice,
      dropAmount: regimeData.dropFromPeak,
      expectedBounce: CONFIG.takeProfitCents,
      takeProfit: current + CONFIG.takeProfitCents,
      stopLoss: current - CONFIG.stopLossCents,
      regime: regimeData.regime,
      ticksSinceNewLow: regimeData.ticksSinceNewLow,
      quality,
      reason: `${regimeData.regime.toUpperCase()}: dropped ${regimeData.dropFromPeak}¢ (${regimeData.peakPrice}→${current}), ${regimeData.ticksSinceNewLow} ticks stable, Q:${quality}`,
      timestamp: now,
      confidence,
    });
  }

  // Sort by quality score descending — only top-ranked trades should be taken
  signals.sort((a, b) => b.quality - a.quality);

  return { signals, regimes, config: CONFIG };
}

module.exports = { evaluate, detectRegime, scoreSignal, CONFIG, STRATEGY_ID };
