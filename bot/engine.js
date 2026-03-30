/**
 * Bot Engine v4.1
 * Adds: paper/live toggle, global drawdown, trade quality filter, validation tracking.
 */

const strategy = require("./strategy");
const { placeLimitOrder } = require("./executor");
const { PositionManager } = require("./position-manager");

const BOT_TICK_INTERVAL = 5000;

class BotEngine {
  constructor(initialBankroll = 10000) {
    this.positionManager = new PositionManager(initialBankroll);
    this.priceHistory = {};
    this.regimes = {};
    this.alerts = [];
    this.isRunning = false;
    this.mode = "manual";
    this.pendingSignals = [];
    this.tickCount = 0;
    this.startedAt = null;
    this._interval = null;
    this._fetchMarkets = null;
  }

  start(fetchMarkets) {
    if (this.isRunning) return;
    this._fetchMarkets = fetchMarkets;
    this.isRunning = true;
    this.startedAt = Date.now();
    this._interval = setInterval(() => this.tick(), BOT_TICK_INTERVAL);
    this.tick();
    const modeLabel = this.positionManager.paperMode ? "PAPER" : "LIVE";
    this._addAlert("system", `Bot started (v4.1 ${modeLabel})`, `Mode: ${this.mode} | SL:-${strategy.CONFIG.stopLossCents}¢ TP:+${strategy.CONFIG.takeProfitCents}¢ | Spread≤${strategy.CONFIG.maxSpread}¢`);
    console.log(`[BOT] Engine v4.1 started in ${this.mode} mode (${modeLabel})`);
  }

  stop() {
    if (this._interval) clearInterval(this._interval);
    this.isRunning = false;
    this._addAlert("system", "Bot stopped", "");
    console.log("[BOT] Engine stopped");
  }

  setMode(mode) {
    if (mode !== "auto" && mode !== "manual") return;
    this.mode = mode;
    this._addAlert("system", `Mode → ${mode}`, "");
  }

  setPaperMode(enabled) {
    this.positionManager.setPaperMode(enabled);
    const label = enabled ? "PAPER" : "LIVE";
    this._addAlert("system", `Trading mode → ${label}`, enabled ? "Orders will be simulated" : "⚠️ REAL ORDERS WILL BE PLACED");
  }

  setRiskPct(pct) {
    this.positionManager.setManualRiskPct(pct);
    this._addAlert("system", `Risk override → ${pct}%`, "");
  }

  resetGlobalPause() {
    this.positionManager.resetGlobalPause();
    this._addAlert("system", "Global pause reset", "Trading resumed");
  }

  async tick() {
    if (!this._fetchMarkets) return;
    this.tickCount++;

    try {
      const markets = await this._fetchMarkets();
      if (!markets || markets.length === 0) return;

      const now = Date.now();
      for (const m of markets) {
        if (!this.priceHistory[m.ticker]) this.priceHistory[m.ticker] = [];
        this.priceHistory[m.ticker].push({ price: m.lastPrice, timestamp: now });
        if (this.priceHistory[m.ticker].length > 120) {
          this.priceHistory[m.ticker].shift();
        }
      }

      // Check position exits (stop-loss always enforced)
      const { exits } = this.positionManager.update(markets);
      for (const exit of exits) {
        const tag = exit.realizedPnl >= 0 ? "✓" : "✗";
        const modeTag = exit.paper ? "[PAPER]" : "[LIVE]";
        this._addAlert("trade_closed", `${tag} ${modeTag} Closed ${exit.ticker}`, `PnL: ${exit.realizedPnl > 0 ? "+" : ""}${exit.realizedPnl}¢ — ${exit.exitReason}`);
      }

      // Global drawdown alert
      if (this.positionManager.globalPaused) {
        if (this.tickCount % 12 === 0) {
          this._addAlert("error", "⛔ TRADING PAUSED", `Global drawdown limit (-${strategy.CONFIG.globalDrawdownPct}%) reached`);
        }
        return;
      }

      const riskState = this.positionManager.getRiskState();
      const canTrade = this.positionManager.canTrade();

      if (!canTrade.allowed) {
        if (this.tickCount % 12 === 0) {
          this._addAlert("system", "Trading paused", canTrade.reason);
        }
        return;
      }

      // Evaluate strategy
      const { signals, regimes } = strategy.evaluate(
        markets,
        this.priceHistory,
        this.positionManager.positions,
        riskState
      );

      this.regimes = { ...this.regimes, ...regimes };

      if (signals.length > 0) {
        // Only take top-ranked signals (quality filter)
        const topSignals = signals.slice(0, CONFIG_MAX_SIGNALS_PER_TICK);

        for (const signal of topSignals) {
          this._addAlert("signal", `${signal.regime.toUpperCase()} → ${signal.ticker} [Q:${signal.quality}]`, signal.reason);

          if (this.mode === "auto") {
            await this._executeSignal(signal);
          } else {
            this.pendingSignals = this.pendingSignals.filter(s => s.ticker !== signal.ticker);
            this.pendingSignals.push(signal);
          }
        }
      }
    } catch (err) {
      console.error("[BOT] Tick error:", err.message);
      this._addAlert("error", "Tick error", err.message);
    }
  }

  async _executeSignal(signal) {
    const canTrade = this.positionManager.canTrade();
    if (!canTrade.allowed) {
      this._addAlert("system", `Blocked: ${signal.ticker}`, canTrade.reason);
      return;
    }

    try {
      const { quantity } = this.positionManager.getPositionSize();

      let order;
      if (this.positionManager.paperMode) {
        // Paper mode: simulate fill instantly
        order = {
          orderId: `paper-${Date.now()}`,
          filledQuantity: quantity,
          status: "filled",
          mock: true,
          paper: true,
        };
        console.log(`[PAPER] Simulated order: BUY ${quantity}x ${signal.ticker} @ ${signal.limitPrice}¢`);
      } else {
        order = await placeLimitOrder({
          ticker: signal.ticker,
          side: signal.side,
          price: signal.limitPrice,
          quantity,
        });
      }

      if (order.filledQuantity > 0) {
        this.positionManager.open({
          ticker: signal.ticker,
          title: signal.title,
          player1: signal.player1,
          player2: signal.player2,
          side: signal.side,
          entryPrice: signal.price,
          quantity: order.filledQuantity,
          orderId: order.orderId,
          strategyId: signal.strategyId,
          signal,
        });

        const modeTag = this.positionManager.paperMode ? "[PAPER]" : "[LIVE]";
        this._addAlert(
          "trade_opened",
          `${modeTag} Opened ${signal.ticker}`,
          `BUY ${order.filledQuantity}x @ ${signal.price}¢ | TP:${signal.takeProfit}¢ SL:${signal.stopLoss}¢ | ${signal.regime} Q:${signal.quality}`
        );
      }
    } catch (err) {
      this._addAlert("error", `Exec failed: ${signal.ticker}`, err.message);
    }
  }

  async confirmSignal(ticker) {
    const signal = this.pendingSignals.find(s => s.ticker === ticker);
    if (!signal) return { error: "No pending signal for this ticker" };
    this.pendingSignals = this.pendingSignals.filter(s => s.ticker !== ticker);
    await this._executeSignal(signal);
    return { success: true };
  }

  dismissSignal(ticker) {
    this.pendingSignals = this.pendingSignals.filter(s => s.ticker !== ticker);
    return { success: true };
  }

  _addAlert(type, title, detail) {
    this.alerts.unshift({
      id: `alert-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      type,
      title,
      detail,
      timestamp: Date.now(),
    });
    if (this.alerts.length > 100) this.alerts.length = 100;
  }

  getState() {
    const posState = this.positionManager.getState();
    return {
      isRunning: this.isRunning,
      mode: this.mode,
      tickCount: this.tickCount,
      uptime: this.startedAt ? Date.now() - this.startedAt : 0,
      ...posState,
      pendingSignals: this.pendingSignals,
      alerts: this.alerts.slice(0, 50),
      strategyConfig: strategy.CONFIG,
      regimes: this.regimes,
    };
  }
}

const CONFIG_MAX_SIGNALS_PER_TICK = 2;

module.exports = { BotEngine };
