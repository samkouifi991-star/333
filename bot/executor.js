/**
 * Mock Order Executor
 * Simulates order placement on Kalshi.
 * Will be replaced with real API calls when ready.
 */

let orderCounter = 0;

/**
 * Place a limit order (mock).
 * @param {{ ticker, side, price, quantity }} order
 * @returns {Promise<Object>} order result
 */
async function placeLimitOrder({ ticker, side, price, quantity = 1 }) {
  // Simulate network latency
  await new Promise((r) => setTimeout(r, 50 + Math.random() * 100));

  orderCounter++;
  const orderId = `mock-${orderCounter}-${Date.now()}`;

  // Simulate partial fills (10% chance)
  const filled = Math.random() < 0.1 ? Math.max(1, Math.floor(quantity * 0.5)) : quantity;

  const order = {
    orderId,
    ticker,
    side,
    price,
    quantity,
    filledQuantity: filled,
    remainingQuantity: quantity - filled,
    status: filled === quantity ? "filled" : "partial",
    createdAt: Date.now(),
    mock: true,
  };

  console.log(`[EXECUTOR] ${order.status.toUpperCase()} order ${orderId}: ${side} ${filled}/${quantity} ${ticker} @ ${price}¢`);
  return order;
}

/**
 * Cancel an order (mock).
 */
async function cancelOrder(orderId) {
  await new Promise((r) => setTimeout(r, 30));
  console.log(`[EXECUTOR] Cancelled order ${orderId}`);
  return { orderId, status: "cancelled" };
}

module.exports = { placeLimitOrder, cancelOrder };
