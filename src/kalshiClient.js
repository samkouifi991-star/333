const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const fetch = require("node-fetch");

// ── Load private key ──────────────────────────────────────────────────
const privateKeyPath = path.join(process.cwd(), "kalshi_private_key.pem");
let privateKey = null;

try {
  const keyData = fs.readFileSync(privateKeyPath, "utf8").trim();
  privateKey = crypto.createPrivateKey(keyData);
  console.log("[AUTH] Private key loaded from", privateKeyPath);
} catch (err) {
  console.error("[AUTH] Failed to load private key:", err.message);
  console.error("[AUTH] Ensure kalshi_private_key.pem exists at", privateKeyPath);
}

const API_KEY_ID = process.env.KALSHI_API_KEY || "";

// ── RSA-PSS Request Signing ───────────────────────────────────────────
function signRequest(timestampMs, method, requestPath) {
  if (!privateKey) throw new Error("Private key not loaded");

  // Kalshi signs: timestamp_ms + METHOD + path (without query params)
  const message = `${timestampMs}${method.toUpperCase()}${requestPath}`;

  const signature = crypto.sign("sha256", Buffer.from(message), {
    key: privateKey,
    padding: crypto.constants.RSA_PKCS1_PSS_PADDING,
    saltLength: crypto.constants.RSA_PSS_SALTLEN_DIGEST, // salt = hash length
  });

  return signature.toString("base64");
}

// ── Build auth headers ────────────────────────────────────────────────
function getAuthHeaders(method, requestPath) {
  const timestampMs = String(Date.now());
  const signature = signRequest(timestampMs, method, requestPath);

  return {
    "KALSHI-ACCESS-KEY": API_KEY_ID,
    "KALSHI-ACCESS-SIGNATURE": signature,
    "KALSHI-ACCESS-TIMESTAMP": timestampMs,
    Accept: "application/json",
    "Content-Type": "application/json",
  };
}

// ── Authenticated fetch wrapper ───────────────────────────────────────
async function kalshiFetch(baseUrl, pathWithQuery, method = "GET", body = null) {
  // Strip query params for signing (Kalshi requirement)
  const pathOnly = pathWithQuery.split("?")[0];
  const fullPath = `/trade-api/v2${pathOnly}`;
  const url = `${baseUrl}/trade-api/v2${pathWithQuery}`;

  const headers = getAuthHeaders(method, fullPath);
  const options = { method, headers };
  if (body) options.body = JSON.stringify(body);

  const res = await fetch(url, options);

  if (!res.ok) {
    const text = await res.text();
    const err = new Error(`Kalshi API ${res.status}: ${text.slice(0, 300)}`);
    err.status = res.status;
    err.responseBody = text;
    throw err;
  }

  return res.json();
}

// ── Check if auth is configured ───────────────────────────────────────
function isConfigured() {
  return !!API_KEY_ID && !!privateKey;
}

module.exports = { kalshiFetch, getAuthHeaders, isConfigured, API_KEY_ID };
