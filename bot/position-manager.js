/**
 * Position Manager v4.1
 * Tracks positions, enforces risk management, handles position sizing and scaling.
 * Adds: paper trading, global drawdown, loss streak tracking, analytics.
 */

const { CONFIG } = require("./strategy");

class PositionManager {
  constructor(initialBankroll = 10000) {
    /** @type {Object.<string, Object>} active positions keyed by ticker */
    this.positions = {};
    /** @type {Array} closed trade history */
    this.history = [];
    this.totalPnl = 0;
    this.dailyPnl = 0;
    this.dailyResetDate = new Date().toDateString();
    this.bankroll = initialBankroll;
    this.initialBankroll = initialBankroll;
    this.peakBankroll = initialBankroll;
    this.maxDrawdown = 0;

    // Paper trading mode (default ON)
    this.paperMode = true;

    // Global drawdown pause
    this.globalPaused = false;

    // Loss streak tracking
    this.currentLossStreak = 0;
    this.longestLossStreak = 0;

    // Manual risk override
    this.manualRiskPct = null; // null = use default
  }

  /** Reset daily PnL at day boundary */
  _checkDayReset() {
    const today = new Date().toDateString();
    if (today !== this.dailyResetDate) {
      this.dailyPnl = 0;
      this.dailyResetDate = today;
    }
  }

  /** Check global drawdown */
  _checkGlobalDrawdown() {
    const drawdownPct = ((this.initialBankroll - this.bankroll) / this.initialBankroll) * 100;
    if (drawdownPct >= CONFIG.globalDrawdownPct) {
      this.globalPaused = true;
    }
  }

  setPaperMode(enabled) {
    this.paperMode = !!enabled;
  }

  setManualRiskPct(pct) {
    if (pct === null || pct === undefined) {
      this.manualRiskPct = null;
    } else {
      this.manualRiskPct = Math.min(Math.max(pct, 0.1), CONFIG.maxRiskPct);
    }
  }

  resetGlobalPause() {
    this.globalPaused = false;
  }

  /** Calculate position size based on bankroll and scaling rules */
  getPositionSize() {
    this._checkDayReset();
    let riskPct = this.manualRiskPct || CONFIG.defaultRiskPct;

    // Scale up only after threshold trades and if profitable
    if (!this.manualRiskPct && this.history.length >= CONFIG.scalingThreshold && this.totalPnl > 0) {
      riskPct = Math.min(riskPct * 1.5, CONFIG.maxRiskPct);
    }

    const riskAmount = this.bankroll * (riskPct / 100);
    const quantity = Math.max(1, Math.floor(riskAmount / CONFIG.stopLossCents));
    return { quantity, riskPct, riskAmount };
  }

  /** Check if trading is allowed */
  canTrade() {
    this._checkDayReset();
    this._checkGlobalDrawdown();

    if (this.globalPaused) {
      return { allowed: false, reason: `GLOBAL DRAWDOWN: -${CONFIG.globalDrawdownPct}% limit hit` };
    }

    const openCount = Object.keys(this.positions).length;
    if (openCount >= CONFIG.maxOpenTrades) {
      return { allowed: false, reason: `Max open trades reached (${openCount}/${CONFIG.maxOpenTrades})` };
    }
    const dailyLimit = this.bankroll * (CONFIG.dailyStopLossPct / 100);
    if (this.dailyPnl <= -dailyLimit) {
      return { allowed: false, reason: `Daily stop-loss hit (${this.dailyPnl.toFixed(0)}¢ / -${dailyLimit.toFixed(0)}¢ limit)` };
    }
    return { allowed: true };
  }

  /** Open a new position */
  open({ ticker, title, player1, player2, side, entryPrice, quantity, orderId, strategyId, signal }) {
    const position = {
      id: `pos-${Date.now()}-${ticker}`,
      ticker,
      title,
      player1,
      player2,
      side,
      entryPrice,
      quantity,
      orderId,
      strategyId,
      regime: signal?.regime || "unknown",
      quality: signal?.quality || 0,
      takeProfit: entryPrice + CONFIG.takeProfitCents,
      stopLoss: entryPrice - CONFIG.stopLossCents,
      openedAt: Date.now(),
      maxHoldUntil: Date.now() + CONFIG.maxPositionAgeMs,
      currentPrice: entryPrice,
      unrealizedPnl: 0,
      status: "open",
      paper: this.paperMode,
    };

    this.positions[ticker] = position;
    const tag = this.paperMode ? "[PAPER]" : "[LIVE]";
    console.log(`${tag}[POS] OPEN: ${side} ${quantity}x ${ticker} @ ${entryPrice}¢ | TP:${position.takeProfit}¢ SL:${position.stopLoss}¢ | regime:${position.regime} Q:${position.quality}`);
    return position;
  }

  /** Update prices and check exits. STOP-LOSS IS NEVER OVERRIDDEN. */
  update(markets) {
    const exits = [];
    const now = Date.now();

    const marketMap = {};
    for (const m of markets) marketMap[m.ticker] = m;

    for (const [ticker, pos] of Object.entries(this.positions)) {
      const market = marketMap[ticker];
      if (!market) continue;

      pos.currentPrice = market.yesBid || market.lastPrice;
      pos.unrealizedPnl = (pos.currentPrice - pos.entryPrice) * pos.quantity;

      let exitReason = null;

      // STOP LOSS — always enforced, no override
      if (pos.currentPrice <= pos.stopLoss) {
        exitReason = `STOP LOSS: ${pos.currentPrice}¢ <= ${pos.stopLoss}¢`;
      }
      // Take profit
      else if (pos.currentPrice >= pos.takeProfit) {
        exitReason = `TAKE PROFIT: ${pos.currentPrice}¢ >= ${pos.takeProfit}¢`;
      }
      // Time exit
      else if (now >= pos.maxHoldUntil) {
        exitReason = `TIME EXIT: held ${Math.round((now - pos.openedAt) / 1000)}s`;
      }
      // Market no longer active
      else if (market.status !== "active") {
        exitReason = `MARKET CLOSED: status=${market.status}`;
      }

      if (exitReason) {
        exits.push(this._close(ticker, pos, exitReason));
      }
    }

    return { exits };
  }

  _close(ticker, pos, reason) {
    const pnl = (pos.currentPrice - pos.entryPrice) * pos.quantity;
    const closedTrade = {
      ...pos,
      exitPrice: pos.currentPrice,
      realizedPnl: pnl,
      exitReason: reason,
      closedAt: Date.now(),
      holdTimeMs: Date.now() - pos.openedAt,
      status: "closed",
    };

    this.totalPnl += pnl;
    this.dailyPnl += pnl;
    this.bankroll += pnl;
    this.peakBankroll = Math.max(this.peakBankroll, this.bankroll);
    this.maxDrawdown = Math.max(this.maxDrawdown, this.peakBankroll - this.bankroll);

    // Loss streak tracking
    if (pnl < 0) {
      this.currentLossStreak++;
      this.longestLossStreak = Math.max(this.longestLossStreak, this.currentLossStreak);
    } else {
      this.currentLossStreak = 0;
    }

    this._checkGlobalDrawdown();

    this.history.push(closedTrade);
    delete this.positions[ticker];

    const tag = pnl >= 0 ? "WIN" : "LOSS";
    const modeTag = pos.paper ? "[PAPER]" : "[LIVE]";
    console.log(`${modeTag}[POS] ${tag}: ${ticker} @ ${pos.currentPrice}¢ | PnL: ${pnl > 0 ? "+" : ""}${pnl}¢ | ${reason}`);
    return closedTrade;
  }

  getRiskState() {
    this._checkDayReset();
    return {
      openTradeCount: Object.keys(this.positions).length,
      dailyPnl: this.dailyPnl,
      bankroll: this.bankroll,
      totalTrades: this.history.length,
      totalPnl: this.totalPnl,
      globalPaused: this.globalPaused,
    };
  }

  getAnalytics() {
    const wins = this.history.filter(t => t.realizedPnl > 0);
    const losses = this.history.filter(t => t.realizedPnl <= 0);
    const avgWin = wins.length > 0 ? wins.reduce((s, t) => s + t.realizedPnl, 0) / wins.length : 0;
    const avgLoss = losses.length > 0 ? losses.reduce((s, t) => s + t.realizedPnl, 0) / losses.length : 0;
    const avgPnl = this.history.length > 0 ? this.totalPnl / this.history.length : 0;
    const drawdownPct = this.initialBankroll > 0
      ? Math.round(((this.initialBankroll - this.bankroll) / this.initialBankroll) * 1000) / 10
      : 0;

    return {
      totalTrades: this.history.length,
      winRate: this.history.length > 0 ? Math.round((wins.length / this.history.length) * 100) : 0,
      avgWin: Math.round(avgWin * 100) / 100,
      avgLoss: Math.round(avgLoss * 100) / 100,
      avgPnl: Math.round(avgPnl * 100) / 100,
      totalPnl: this.totalPnl,
      maxDrawdown: Math.round(this.maxDrawdown),
      drawdownPct,
      currentLossStreak: this.currentLossStreak,
      longestLossStreak: this.longestLossStreak,
      paperTrades: this.history.filter(t => t.paper).length,
      liveTrades: this.history.filter(t => !t.paper).length,
    };
  }

  getState() {
    this._checkDayReset();
    const analytics = this.getAnalytics();
    return {
      activePositions: Object.values(this.positions),
      tradeHistory: this.history.slice(-50),
      totalPnl: this.totalPnl,
      dailyPnl: this.dailyPnl,
      bankroll: Math.round(this.bankroll),
      maxDrawdown: Math.round(this.maxDrawdown),
      winRate: analytics.winRate,
      totalTrades: this.history.length,
      positionSizing: this.getPositionSize(),
      canTrade: this.canTrade(),
      paperMode: this.paperMode,
      globalPaused: this.globalPaused,
      analytics,
      validationProgress: {
        tradesCompleted: this.history.length,
        tradesRequired: 200,
        ready: this.history.length >= 200 && analytics.avgPnl > 0 && analytics.winRate >= 38,
      },
    };
  }
}

module.exports = { PositionManager };
