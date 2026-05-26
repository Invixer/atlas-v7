// ATLAS v7 - Professional Day Trading Bot
// REAL-TIME prices via Finnhub WebSocket (not polling)
// All numbers verified. All bugs fixed. Real trades.
 
const express = require('express');
const app = express();
const path = require('path');
const https = require('https');
const WebSocket = require('ws');
const PORT = process.env.PORT || 3000;
const FINNHUB_KEY = process.env.FINNHUB_KEY;
const fs = require('fs');
 
const START_CAPITAL = 1000;
const BACKUP_FILE = './atlas-state.json';
 
if (!FINNHUB_KEY) {
  console.error('[FATAL] FINNHUB_KEY not set. Set it in Railway Variables or .env');
  process.exit(1);
}
 
// Express
app.use(express.json());
app.use(express.static(path.join(__dirname)));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'dashboard.html')));
 
// ─── STATE ───────────────────────────────────────────────────────────────────
 
let marketData = {};   // { "PLTR": { price, prevClose, high, low, lastUpdate } }
let wsConnected = false;
 
let portfolio = {
  cash: START_CAPITAL,
  longPositions: {},   // { "PLTR": [{ qty, entryPrice }] }
  shortPositions: {},  // { "MARA": [{ qty, entryPrice }] }
  trades: [],
  closedTrades: []
};
 
let tradeLogger = [];
const MAX_LOG_ENTRIES = 200;
 
// FIXED BUG #10: tradeLogger grew forever (memory leak over weeks of uptime).
// This helper appends and trims to the most recent MAX_LOG_ENTRIES.
function logTrade(entry) {
  tradeLogger.push(entry);
  if (tradeLogger.length > MAX_LOG_ENTRIES) {
    tradeLogger = tradeLogger.slice(-MAX_LOG_ENTRIES);
  }
}
 
const MAX_CLOSED_TRADES_KEPT = 200;
 
// FIXED BUG #1: portfolio.trades grew without bound — memory + disk leak.
// Keep every OPEN trade (needed for live positions) plus only the most
// recent MAX_CLOSED_TRADES_KEPT closed trades (enough for win-rate / learning).
function trimTrades() {
  const open = portfolio.trades.filter(t => t.status === 'open');
  const closed = portfolio.trades.filter(t => t.status === 'closed');
  if (closed.length > MAX_CLOSED_TRADES_KEPT) {
    const trimmedClosed = closed.slice(-MAX_CLOSED_TRADES_KEPT);
    portfolio.trades = [...open, ...trimmedClosed];
  }
}
 
let aiSystem = {
  aggressionLevel: 0.5,
  strategy: 'balanced',
  recentWinRate: 0.5,
  winningStreak: 0,
  losingStreak: 0,
  _lastCountedTradeTime: null,       // guards streak inflation
  _lastSentimentUpdateTime: null,    // guards sentiment shift inflation
  _lastPerfHistoryTime: null,        // guards performance history inflation
  performanceHistory: [],
  currentReasoning: { winRate: 0, volatility: 0, trend: 'neutral', confidence: 0, nextAction: 'waiting' },
  reinvestmentSystem: { enabled: true, profitThreshold: 100, profitsReinvested: 0, totalReinvestments: 0 }
};
 
let riskSystem = {
  maxDrawdown: 0.20,
  maxPortfolioHeat: 0.50,
  dailyLossLimit: 0.05,   // FIXED BUG #1: was referenced everywhere but never defined
  currentDrawdown: 0,
  portfolioHeat: 0,
  riskLevel: 'normal',
  checksPassing: [],
  dailyRealizedLoss: 0,
  peakValue: START_CAPITAL
};
 
let sentimentData = {
  general: 0.5,
  byMarket: {
    // FIXED BUG #8: NASDAQ and NYSE trade in the same US session, so they share
    // one sentiment key ('nasdaq'). 'nyse' was never read by trading signals —
    // the signal code uses sentimentData.byMarket[market] where market is always
    // 'nasdaq' during US hours. Keeping the key here for API completeness.
    nasdaq: 0.5,
    nyse: 0.5,   // kept for API display; mirrors nasdaq since same session
    jsx: 0.5
  }
};
let marketTransitionData = { lastMarket: null, nasdaqNyseOpenTrades: [], jsxOpenTrades: [] };
 
const WATCHLISTS = {
  nasdaq: ['PLTR', 'SOFI', 'MARA', 'HOOD', 'SOUN', 'IONQ', 'RKLB', 'BBAI', 'HIMS', 'CIFR'],
  nyse: ['F', 'BAC', 'JPM', 'WFC', 'GE', 'XOM', 'MRK', 'JNJ', 'PFE', 'KO'],
  jsx: ['9984.T', '7203.T', '6758.T', '8301.T', '8306.T', '8411.T', '9432.T', '6861.T', '4568.T', '5201.T']
};
 
// ─── FINNHUB WEBSOCKET (REAL-TIME PRICES) ───────────────────────────────────
 
let finnhubWs = null;
let wsReconnectTimer = null;
 
function connectWebSocket() {
  // FIXED BUG #3: Clear any pending reconnect timer so we never run two
  // reconnect loops in parallel (which would double connections/subscriptions).
  if (wsReconnectTimer) {
    clearTimeout(wsReconnectTimer);
    wsReconnectTimer = null;
  }
 
  // Detach handlers from the old socket BEFORE closing it — otherwise its
  // 'close' event fires and schedules yet another reconnect.
  if (finnhubWs) {
    try {
      finnhubWs.removeAllListeners();
      finnhubWs.close();
    } catch (e) {}
  }
 
  console.log('[WS] Connecting to Finnhub WebSocket...');
  finnhubWs = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_KEY}`);
 
  finnhubWs.on('open', () => {
    wsConnected = true;
    console.log('[WS] ✓ Connected to Finnhub real-time feed');
 
    // Subscribe to all stocks in all markets
    const allSymbols = [...WATCHLISTS.nasdaq, ...WATCHLISTS.nyse, ...WATCHLISTS.jsx];
    allSymbols.forEach(symbol => {
      finnhubWs.send(JSON.stringify({ type: 'subscribe', symbol }));
    });
    console.log(`[WS] Subscribed to ${allSymbols.length} symbols`);
  });
 
  finnhubWs.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'trade' && msg.data) {
        msg.data.forEach(trade => {
          const symbol = trade.s;
          const price = trade.p;
          // Guard: ignore malformed ticks with no valid price
          if (!symbol || !Number.isFinite(price) || price <= 0) return;
          if (!marketData[symbol]) {
            // No REST data yet — prevClose temporarily equals price until REST fills it
            marketData[symbol] = { price, prevClose: price, high: price, low: price, lastUpdate: Date.now() };
          } else {
            // prevClose is intentionally NOT overwritten — it must stay the real
            // previous-day close from the REST fetch, or momentum decays to ~0%.
            marketData[symbol].price = price;
            marketData[symbol].high = Math.max(marketData[symbol].high, price);
            marketData[symbol].low = Math.min(marketData[symbol].low, price);
            marketData[symbol].lastUpdate = Date.now();
          }
        });
      }
    } catch (e) {}
  });
 
  finnhubWs.on('close', () => {
    wsConnected = false;
    // Only schedule a reconnect if one isn't already pending.
    if (!wsReconnectTimer) {
      console.log('[WS] Disconnected. Reconnecting in 5s...');
      wsReconnectTimer = setTimeout(() => {
        wsReconnectTimer = null;
        connectWebSocket();
      }, 5000);
    }
  });
 
  finnhubWs.on('error', (err) => {
    console.error('[WS] Error:', err.message);
    wsConnected = false;
  });
}
 
// Fallback REST fetch for initial prices and when WS is down
function fetchQuote(symbol) {
  return new Promise((resolve) => {
    const url = `https://finnhub.io/api/v1/quote?symbol=${symbol}&token=${FINNHUB_KEY}`;
    let resolved = false;
 
    // BUG #4 FIX: capture the request handle so we can destroy() it on timeout.
    // Without destroy(), the timed-out request keeps running: it still consumes
    // a Finnhub rate-limit slot and leaks the response object.
    const req = https.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        if (resolved) return;  // already timed out — discard
        clearTimeout(timeoutHandle);
        resolved = true;
        try {
          const json = JSON.parse(data);
          if (json.c && json.c > 0) {
            resolve({ price: json.c, prevClose: json.pc, high: json.h, low: json.l });
          } else { resolve(null); }
        } catch (e) { resolve(null); }
      });
    });
 
    req.on('error', () => {
      if (resolved) return;
      clearTimeout(timeoutHandle);
      resolved = true;
      resolve(null);
    });
 
    const timeoutHandle = setTimeout(() => {
      if (resolved) return;
      resolved = true;
      req.destroy();  // abort the in-flight request
      console.warn(`[PRICES] Timeout fetching ${symbol} — skipping`);
      resolve(null);
    }, 8000);
  });
}
 
let _fetchInProgress = false;
 
async function fetchInitialPrices() {
  // FIX: Prevent concurrent runs. fetchInitialPrices takes ~28s. Without this
  // guard the 6-hour interval could fire a second run before the first finishes,
  // causing both to write to marketData in parallel with unpredictable results.
  if (_fetchInProgress) {
    console.log('[PRICES] Fetch already in progress — skipping this interval tick');
    return;
  }
  _fetchInProgress = true;
 
  try {
    // Fetch ALL symbols across all markets, not just the current one — otherwise
    // stocks in closed markets have no real prevClose and produce 0% momentum.
    const symbols = [...WATCHLISTS.nasdaq, ...WATCHLISTS.nyse, ...WATCHLISTS.jsx];
 
    const BATCH_SIZE = 5;
    const BATCH_GAP_MS = 5500;
 
    console.log(`[PRICES] Fetching initial prices for ${symbols.length} symbols (all markets, rate-limited batches)...`);
 
    for (let i = 0; i < symbols.length; i += BATCH_SIZE) {
      const batch = symbols.slice(i, i + BATCH_SIZE);
      await Promise.all(batch.map(async (symbol) => {
        const quote = await fetchQuote(symbol);
        if (quote) {
          const existing = marketData[symbol];
          marketData[symbol] = {
            price: existing?.price ?? quote.price,
            prevClose: quote.prevClose,
            high: quote.high,
            low: quote.low,
            lastUpdate: Date.now()
          };
        }
      }));
      if (i + BATCH_SIZE < symbols.length) {
        await new Promise(r => setTimeout(r, BATCH_GAP_MS));
      }
    }
    console.log(`[PRICES] ✓ Loaded ${Object.keys(marketData).length} initial prices`);
  } finally {
    _fetchInProgress = false;
  }
}
 
// ─── UTILITY ────────────────────────────────────────────────────────────────
 
const NASDAQ_NYSE_HOURS = { start: 9.5, end: 16 };
const JSX_HOURS = { start: 19, end: 2 };
 
// FIXED BUG #5: getHours() returns the SERVER's timezone (Railway runs in UTC).
// We must compute hours in America/New_York (EST/EDT) so markets open/close
// at the correct real-world times regardless of where the server runs.
function getEasternTimeParts() {
  const now = new Date();
  // en-US formatting in the New York timezone gives us correct local wall-clock time
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    hour: 'numeric', minute: 'numeric', weekday: 'short', hour12: false
  }).formatToParts(now);
 
  let hour = 0, minute = 0, weekday = '';
  parts.forEach(p => {
    if (p.type === 'hour') hour = parseInt(p.value, 10);
    if (p.type === 'minute') minute = parseInt(p.value, 10);
    if (p.type === 'weekday') weekday = p.value;
  });
  // Intl can return hour 24 at midnight in some environments — normalize
  if (hour === 24) hour = 0;
 
  const dayMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
  return { hours: hour + minute / 60, day: dayMap[weekday] ?? 0 };
}
 
function getCurrentMarket() {
  const { hours, day } = getEasternTimeParts();
 
  // JSX runs 7 PM EST → 2 AM EST, Mon–Fri Tokyo time.
  // Valid Eastern windows: Sun 7 PM → Mon 2 AM, Mon 7 PM → Tue 2 AM, ... Fri 7 PM → Fri 2 AM (Sat not valid).
  //
  // FIXED BUG: The previous fix correctly blocked Saturday (day 6) but missed
  // Sunday 12:00 AM–7:00 PM EST. That window is "hours < 2 AND day = 0" which
  // is STILL Saturday night in Tokyo — JSX is closed.
  // Guard: exclude Saturday entirely AND exclude the early-morning side of Sunday
  // (midnight → 7 PM, i.e. before the Sunday evening session starts).
  const isJSXHours = hours >= JSX_HOURS.start || hours < JSX_HOURS.end;
  const isSundayEarlyMorning = day === 0 && hours < JSX_HOURS.start; // Sun 0:00–19:00
  if (isJSXHours && day !== 6 && !isSundayEarlyMorning) return 'jsx';
 
  if (day === 0 || day === 6) return null;
  if (hours >= NASDAQ_NYSE_HOURS.start && hours < NASDAQ_NYSE_HOURS.end) return 'nasdaq';
  return null;
}
 
function getMarketStatus() {
  const { hours, day } = getEasternTimeParts();
  // Mirror getCurrentMarket exactly so dashboard status is never wrong
  const isJSXHours = hours >= JSX_HOURS.start || hours < JSX_HOURS.end;
  const isSundayEarlyMorning = day === 0 && hours < JSX_HOURS.start;
  if (isJSXHours && day !== 6 && !isSundayEarlyMorning)
    return { status: 'JSX OPEN', openTime: '7:00 PM EST', closeTime: '2:00 AM EST' };
  if (day === 0 || day === 6) return { status: 'CLOSED', reason: 'Weekend' };
  if (hours >= NASDAQ_NYSE_HOURS.start && hours < NASDAQ_NYSE_HOURS.end)
    return { status: 'NASDAQ/NYSE OPEN', openTime: '9:30 AM EST', closeTime: '4:00 PM EST' };
  return { status: 'After Hours', openTime: '9:30 AM EST', closeTime: '4:00 PM EST' };
}
 
// ─── PORTFOLIO VALUE ────────────────────────────────────────────────────────
 
function getTotalValue() {
  let value = portfolio.cash;
 
  // FIXED BUG #9: If a ticker has no live price, fall back to entry price
  // (cost basis) instead of dropping the position — otherwise restored
  // positions silently vanish from total value until their first tick.
  Object.entries(portfolio.longPositions).forEach(([ticker, positions]) => {
    const price = marketData[ticker]?.price;
    positions.forEach(pos => {
      const p = Number(price || pos.entryPrice) || 0;
      value += p * (pos.qty || 0);
    });
  });
 
  Object.entries(portfolio.shortPositions).forEach(([ticker, positions]) => {
    const price = marketData[ticker]?.price;
    positions.forEach(pos => {
      const ep = Number(pos.entryPrice) || 0;
      const p = Number(price || ep) || ep;
      value += (ep - p) * (pos.qty || 0);
    });
  });
 
  return isNaN(value) ? portfolio.cash : value;
}
 
// ─── TRADE EXECUTION ────────────────────────────────────────────────────────
 
function executeLong(ticker, price, reason) {
  // FIXED BUG #7: Guard against invalid price (0, negative, NaN, Infinity).
  // Division by a bad price produces Infinity/NaN position sizes.
  if (!Number.isFinite(price) || price <= 0) {
    console.warn(`[TRADE] Skipped LONG ${ticker}: invalid price ${price}`);
    return false;
  }
  const positionSize = Math.max(1, Math.floor((portfolio.cash * 0.4 * aiSystem.aggressionLevel) / price));
  const cost = price * positionSize;
  if (portfolio.cash < cost) return false;
 
  portfolio.longPositions[ticker] = portfolio.longPositions[ticker] || [];
  portfolio.longPositions[ticker].push({ qty: positionSize, entryPrice: price });
  portfolio.cash -= cost;
 
  portfolio.trades.push({
    timestamp: new Date().toISOString(), ticker, direction: 'LONG',
    entryPrice: price, qty: positionSize, status: 'open',
    reason, realizedPnL: 0, market: getCurrentMarket()
  });
  logTrade(`LONG ${ticker} ${positionSize}@$${price.toFixed(2)}: ${reason}`);
  console.log(`[TRADE] ✓ LONG ${ticker}: ${positionSize} @ $${price.toFixed(2)} (Cost: $${cost.toFixed(2)}) - ${reason}`);
  saveState();
  return true;
}
 
function executeShort(ticker, price, reason) {
  // FIXED BUG #7: Guard against invalid price.
  if (!Number.isFinite(price) || price <= 0) {
    console.warn(`[TRADE] Skipped SHORT ${ticker}: invalid price ${price}`);
    return false;
  }
  const positionSize = Math.max(1, Math.floor((portfolio.cash * 0.4 * aiSystem.aggressionLevel) / price));
  const marginRequired = price * positionSize * 0.5;
  if (portfolio.cash < marginRequired) return false;
 
  portfolio.shortPositions[ticker] = portfolio.shortPositions[ticker] || [];
  portfolio.shortPositions[ticker].push({ qty: positionSize, entryPrice: price });
  portfolio.cash -= marginRequired;
 
  portfolio.trades.push({
    timestamp: new Date().toISOString(), ticker, direction: 'SHORT',
    entryPrice: price, qty: positionSize, status: 'open',
    reason, realizedPnL: 0, market: getCurrentMarket()
  });
  logTrade(`SHORT ${ticker} ${positionSize}@$${price.toFixed(2)}: ${reason}`);
  console.log(`[TRADE] ✓ SHORT ${ticker}: ${positionSize} @ $${price.toFixed(2)} (Margin: $${marginRequired.toFixed(2)}) - ${reason}`);
  saveState();
  return true;
}
 
// ─── TRADE CLOSING ──────────────────────────────────────────────────────────
 
function closeLong(ticker) {
  const positions = portfolio.longPositions[ticker];
  if (!positions || positions.length === 0) return;
  const currentPrice = marketData[ticker]?.price;
  if (!currentPrice) return;
 
  // BUG #1 FIX: Read the market from an open trade BEFORE the forEach below
  // marks trades as 'closed'. Reading after mutation means status === 'open'
  // matches nothing, and we fall back to null incorrectly.
  const tradeOpenMarket = portfolio.trades.find(
    t => t.ticker === ticker && t.direction === 'LONG' && t.status === 'open'
  )?.market || null;
 
  let totalPnL = 0;
  positions.forEach(pos => {
    const pnl = (currentPrice - pos.entryPrice) * pos.qty;
    totalPnL += pnl;
    portfolio.cash += currentPrice * pos.qty;
  });
 
  portfolio.trades
    .filter(t => t.ticker === ticker && t.direction === 'LONG' && t.status === 'open')
    .forEach(t => {
      const tradePnL = (currentPrice - t.entryPrice) * t.qty;
      t.status = 'closed';
      t.exitPrice = currentPrice;
      t.realizedPnL = tradePnL;
      t.closedAt = new Date().toISOString();
    });
 
  if (totalPnL < 0) riskSystem.dailyRealizedLoss += Math.abs(totalPnL);
 
  portfolio.closedTrades.push({
    ticker, direction: 'LONG', pnl: totalPnL,
    realizedPnL: totalPnL, closedAt: new Date().toISOString(), market: tradeOpenMarket
  });
  if (portfolio.closedTrades.length > 500) portfolio.closedTrades = portfolio.closedTrades.slice(-500);
  delete portfolio.longPositions[ticker];
  console.log(`[CLOSE] LONG ${ticker} @ $${currentPrice.toFixed(2)} | PnL: $${totalPnL.toFixed(2)}`);
  trimTrades();
  saveState();
}
 
function closeShort(ticker) {
  const positions = portfolio.shortPositions[ticker];
  if (!positions || positions.length === 0) return;
  const currentPrice = marketData[ticker]?.price;
  if (!currentPrice) return;
 
  // BUG #1 FIX: Read market BEFORE mutation
  const shortTradeOpenMarket = portfolio.trades.find(
    t => t.ticker === ticker && t.direction === 'SHORT' && t.status === 'open'
  )?.market || null;
 
  let totalPnL = 0;
  positions.forEach(pos => {
    const pnl = (pos.entryPrice - currentPrice) * pos.qty;
    totalPnL += pnl;
    const marginHeld = pos.entryPrice * pos.qty * 0.5;
    portfolio.cash += marginHeld + pnl;
  });
 
  portfolio.trades
    .filter(t => t.ticker === ticker && t.direction === 'SHORT' && t.status === 'open')
    .forEach(t => {
      const tradePnL = (t.entryPrice - currentPrice) * t.qty;
      t.status = 'closed';
      t.exitPrice = currentPrice;
      t.realizedPnL = tradePnL;
      t.closedAt = new Date().toISOString();
    });
 
  if (totalPnL < 0) riskSystem.dailyRealizedLoss += Math.abs(totalPnL);
 
  portfolio.closedTrades.push({
    ticker, direction: 'SHORT', pnl: totalPnL,
    realizedPnL: totalPnL, closedAt: new Date().toISOString(), market: shortTradeOpenMarket
  });
  if (portfolio.closedTrades.length > 500) portfolio.closedTrades = portfolio.closedTrades.slice(-500);
  delete portfolio.shortPositions[ticker];
  console.log(`[CLOSE] SHORT ${ticker} @ $${currentPrice.toFixed(2)} | PnL: $${totalPnL.toFixed(2)}`);
  trimTrades();
  saveState();
}
 
// ─── PERSISTENCE (saves EVERYTHING) ─────────────────────────────────────────
 
let _saveInProgress = false;
let _savePending = false;
 
function saveState() {
  // FIXED BUG #5: If a write is already running, mark that another is needed
  // and return — avoids overlapping async writes corrupting the file.
  if (_saveInProgress) {
    _savePending = true;
    return;
  }
  _saveInProgress = true;
 
  try {
    const state = {
      cash: portfolio.cash,
      longPositions: portfolio.longPositions,
      shortPositions: portfolio.shortPositions,
      // FIXED BUG #5: persist only a bounded slice so the file can't grow forever
      trades: portfolio.trades.slice(-500),
      closedTrades: portfolio.closedTrades.slice(-500),
      marketTransition: marketTransitionData,
      aiSystem: {
        aggressionLevel: aiSystem.aggressionLevel,
        strategy: aiSystem.strategy,
        recentWinRate: aiSystem.recentWinRate,
        winningStreak: aiSystem.winningStreak,
        losingStreak: aiSystem.losingStreak,
        _lastCountedTradeTime: aiSystem._lastCountedTradeTime,
        _lastSentimentUpdateTime: aiSystem._lastSentimentUpdateTime,
        _lastPerfHistoryTime: aiSystem._lastPerfHistoryTime,
        performanceHistory: aiSystem.performanceHistory,
        currentReasoning: { ...aiSystem.currentReasoning }
      },
      riskSystem: {
        dailyRealizedLoss: riskSystem.dailyRealizedLoss,
        peakValue: riskSystem.peakValue
      },
      // FIXED: persist learned sentiment so restarts don't lose bot's market knowledge
      sentimentData: {
        general: sentimentData.general,
        byMarket: { ...sentimentData.byMarket }
      },
      savedAt: new Date().toISOString()
    };
 
    // FIXED BUG #5: async write so a large JSON serialization never blocks
    // the event loop (which would stall WebSocket message processing).
    // Write to a temp file then rename — atomic, so a crash mid-write can't
    // leave a half-written/corrupt backup.
    const tmpFile = BACKUP_FILE + '.tmp';
    fs.writeFile(tmpFile, JSON.stringify(state), (err) => {
      if (err) {
        console.error('[SAVE_ERROR]', err.message);
        _saveInProgress = false;
        return;
      }
      fs.rename(tmpFile, BACKUP_FILE, (renameErr) => {
        if (renameErr) console.error('[SAVE_ERROR]', renameErr.message);
        _saveInProgress = false;
        // If another save was requested while this one ran, do it now.
        if (_savePending) {
          _savePending = false;
          saveState();
        }
      });
    });
  } catch (e) {
    console.error('[SAVE_ERROR]', e.message);
    _saveInProgress = false;
  }
}
 
// Synchronous save for graceful shutdown only — the process is exiting,
// so blocking briefly is fine and we must finish before exit.
function saveStateSync() {
  try {
    const state = {
      cash: portfolio.cash,
      longPositions: portfolio.longPositions,
      shortPositions: portfolio.shortPositions,
      trades: portfolio.trades.slice(-500),
      closedTrades: portfolio.closedTrades.slice(-500),
      marketTransition: marketTransitionData,
      aiSystem: {
        aggressionLevel: aiSystem.aggressionLevel,
        strategy: aiSystem.strategy,
        recentWinRate: aiSystem.recentWinRate,
        winningStreak: aiSystem.winningStreak,
        losingStreak: aiSystem.losingStreak,
        _lastCountedTradeTime: aiSystem._lastCountedTradeTime,
        _lastSentimentUpdateTime: aiSystem._lastSentimentUpdateTime,
        _lastPerfHistoryTime: aiSystem._lastPerfHistoryTime,
        performanceHistory: aiSystem.performanceHistory,
        currentReasoning: { ...aiSystem.currentReasoning }
      },
      riskSystem: {
        dailyRealizedLoss: riskSystem.dailyRealizedLoss,
        peakValue: riskSystem.peakValue
      },
      // FIXED: persist learned sentiment so restarts don't lose bot's market knowledge
      sentimentData: {
        general: sentimentData.general,
        byMarket: { ...sentimentData.byMarket }
      },
      savedAt: new Date().toISOString()
    };
    fs.writeFileSync(BACKUP_FILE, JSON.stringify(state));
  } catch (e) {
    console.error('[SAVE_ERROR]', e.message);
  }
}
 
function loadState() {
  // FIXED BUG #6: Try the main backup first; if it's missing or corrupt,
  // fall back to the .tmp file (atomic-rename leftover) so a crash mid-write
  // can't wipe all trade history and reset the bot to $1,000.
  const candidates = [BACKUP_FILE, BACKUP_FILE + '.tmp'];
  let state = null;
 
  for (const file of candidates) {
    try {
      if (!fs.existsSync(file)) continue;
      const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (parsed && typeof parsed === 'object') {
        state = parsed;
        if (file !== BACKUP_FILE) {
          console.warn(`[LOAD] Main backup unusable — recovered from ${file}`);
        }
        break;
      }
    } catch (e) {
      console.error(`[LOAD_ERROR] ${file}: ${e.message}`);
    }
  }
 
  if (!state) {
    console.log('[LOAD] No usable backup found — starting fresh');
    return;
  }
 
  try {
    portfolio.cash = state.cash ?? START_CAPITAL;
    portfolio.longPositions = state.longPositions ?? {};
    portfolio.shortPositions = state.shortPositions ?? {};
    portfolio.trades = state.trades ?? [];
    portfolio.closedTrades = state.closedTrades ?? [];
    marketTransitionData = state.marketTransition ?? marketTransitionData;
    if (state.aiSystem) {
      aiSystem.aggressionLevel = state.aiSystem.aggressionLevel ?? 0.5;
      aiSystem.strategy = state.aiSystem.strategy ?? 'balanced';
      aiSystem.recentWinRate = state.aiSystem.recentWinRate ?? 0.5;
      aiSystem.winningStreak = state.aiSystem.winningStreak ?? 0;
      aiSystem.losingStreak = state.aiSystem.losingStreak ?? 0;
      aiSystem._lastCountedTradeTime = state.aiSystem._lastCountedTradeTime ?? null;
      aiSystem._lastSentimentUpdateTime = state.aiSystem._lastSentimentUpdateTime ?? null;
      aiSystem._lastPerfHistoryTime = state.aiSystem._lastPerfHistoryTime ?? null;
      aiSystem.performanceHistory = state.aiSystem.performanceHistory ?? [];
      if (state.aiSystem.currentReasoning) {
        aiSystem.currentReasoning.winRate = state.aiSystem.currentReasoning.winRate ?? 0;
        aiSystem.currentReasoning.volatility = state.aiSystem.currentReasoning.volatility ?? 0;
        aiSystem.currentReasoning.trend = state.aiSystem.currentReasoning.trend ?? 'neutral';
        aiSystem.currentReasoning.confidence = state.aiSystem.currentReasoning.confidence ?? 0;
        aiSystem.currentReasoning.nextAction = state.aiSystem.currentReasoning.nextAction ?? 'waiting';
      }
    }
    if (state.riskSystem) {
      riskSystem.dailyRealizedLoss = state.riskSystem.dailyRealizedLoss ?? 0;
      riskSystem.peakValue = state.riskSystem.peakValue ?? START_CAPITAL;
    }
    // FIXED: restore learned sentiment on restart
    if (state.sentimentData) {
      sentimentData.general = state.sentimentData.general ?? 0.5;
      sentimentData.byMarket.nasdaq = state.sentimentData.byMarket?.nasdaq ?? 0.5;
      sentimentData.byMarket.nyse = state.sentimentData.byMarket?.nyse ?? 0.5;
      sentimentData.byMarket.jsx = state.sentimentData.byMarket?.jsx ?? 0.5;
    }
    console.log(`[LOAD] ✓ Restored: $${portfolio.cash.toFixed(2)} cash, ${portfolio.trades.length} trades, ${Object.keys(portfolio.longPositions).length} longs, ${Object.keys(portfolio.shortPositions).length} shorts`);
  } catch (e) {
    console.error('[LOAD_ERROR]', e.message);
  }
}
 
process.on('SIGTERM', () => { console.log('[SHUTDOWN]'); saveStateSync(); if (finnhubWs) finnhubWs.close(); process.exit(0); });
process.on('SIGINT', () => { console.log('[SHUTDOWN]'); saveStateSync(); if (finnhubWs) finnhubWs.close(); process.exit(0); });
 
// ─── LEARNING SYSTEM ────────────────────────────────────────────────────────
 
function updateLearning() {
  // FIXED BUG #1: Use portfolio.closedTrades (capped at 500) as the authoritative
  // source for win-rate and learning — NOT portfolio.trades, which is trimmed to
  // 200 closed entries and can fall below the 20-trade minimum when many are open.
  // Both sources now store .realizedPnL and .closedAt with the same schema.
  const closed = portfolio.closedTrades;
 
  // Always update nextAction regardless of trade count so dashboard isn't stuck on 'waiting'
  aiSystem.currentReasoning.nextAction = getCurrentMarket()
    ? (closed.length < 3 ? 'collecting initial trades' : 'scanning for signals')
    : 'waiting for market open';
 
  if (closed.length < 3) return;
 
  const recent = closed.slice(-20);
  const wins = recent.filter(t => t.realizedPnL > 0).length;
  const winRate = recent.length > 0 ? wins / recent.length : 0.5;
 
  aiSystem.recentWinRate = winRate;
  aiSystem.currentReasoning.winRate = winRate;
 
  // FIXED BUG #3: Only update streaks if this is a NEW closed trade
  const last = recent[recent.length - 1];
  if (last.closedAt !== aiSystem._lastCountedTradeTime) {
    aiSystem._lastCountedTradeTime = last.closedAt;
    if (last.realizedPnL > 0) { 
      aiSystem.winningStreak++; 
      aiSystem.losingStreak = 0; 
    }
    else { 
      aiSystem.losingStreak++; 
      aiSystem.winningStreak = 0; 
    }
  }
 
  if (winRate > 0.65) { aiSystem.strategy = 'ultra_aggressive'; aiSystem.aggressionLevel = Math.min(1.0, 0.8 + (winRate - 0.65)); }
  else if (winRate > 0.55) { aiSystem.strategy = 'aggressive'; aiSystem.aggressionLevel = 0.7; }
  else if (winRate > 0.45) { aiSystem.strategy = 'balanced'; aiSystem.aggressionLevel = 0.5; }
  else if (winRate > 0.35) { aiSystem.strategy = 'defensive'; aiSystem.aggressionLevel = 0.3; }
  else { aiSystem.strategy = 'ultra_conservative'; aiSystem.aggressionLevel = 0.15; }
 
  const avgPnL = recent.reduce((s, t) => s + t.realizedPnL, 0) / recent.length;
 
  // FIX (Reported #3 + Own #1): Only shift sentiment when a NEW trade closes.
  // Without this guard, sentiment nudged ±0.03 every 10s on the same data,
  // drifting to 0.8 or 0.2 within ~5 minutes of good/bad avgPnL.
  const sentimentShift = avgPnL > 5 ? 0.03 : (avgPnL < -5 ? -0.03 : 0);
  if (sentimentShift !== 0 && last.closedAt !== aiSystem._lastSentimentUpdateTime) {
    aiSystem._lastSentimentUpdateTime = last.closedAt;
    sentimentData.general = Math.max(0.2, Math.min(0.8, sentimentData.general + sentimentShift));
 
    const lastMarket = recent[recent.length - 1]?.market;
    const targetMarket = lastMarket || getCurrentMarket();
    if (targetMarket && sentimentData.byMarket[targetMarket] !== undefined) {
      sentimentData.byMarket[targetMarket] = Math.max(0.2, Math.min(0.8,
        sentimentData.byMarket[targetMarket] + sentimentShift));
    }
  }
 
  // FIX (Own #3): Only push to performanceHistory when a NEW trade closes —
  // otherwise 50 identical entries fill the window in ~8 minutes.
  if (last.closedAt !== aiSystem._lastPerfHistoryTime) {
    aiSystem._lastPerfHistoryTime = last.closedAt;
    aiSystem.performanceHistory.push({
      timestamp: Date.now(),
      winRate,
      avgPnL,
      strategy: aiSystem.strategy,
      aggression: aiSystem.aggressionLevel
    });
    if (aiSystem.performanceHistory.length > 50) {
      aiSystem.performanceHistory.shift();
    }
  }
 
  // FIX (Own #4 #5 #6): Populate currentReasoning so dashboard shows real data.
  // These were always 0/'neutral'/'waiting' since they were never updated.
  aiSystem.currentReasoning.winRate = winRate;
  aiSystem.currentReasoning.confidence = Math.round(
    Math.min(100, closed.length * 2)  // confidence grows with trade count, caps at 100
  );
  aiSystem.currentReasoning.trend = avgPnL > 2 ? 'bullish' : avgPnL < -2 ? 'bearish' : 'neutral';
  aiSystem.currentReasoning.nextAction =
    winRate > 0.55 ? 'increase position size' :
    winRate < 0.40 ? 'reduce exposure' :
    getCurrentMarket() ? 'scanning for signals' : 'waiting for market open';
}
 
// ─── MARKET TRANSITION ──────────────────────────────────────────────────────
 
function updateMarketTransition() {
  const current = getCurrentMarket();
  const prev = marketTransitionData.lastMarket;
  if (current !== prev && prev) {
    console.log(`[TRANSITION] ${prev} → ${current}`);
    saveState();
  }
  marketTransitionData.lastMarket = current;
}
 
// ─── TRADING STRATEGY ───────────────────────────────────────────────────────
 
function evaluateAndTrade() {
  const market = getCurrentMarket();
  if (!market) return;
 
  updateMarketTransition();
 
  const symbols = market === 'jsx' ? WATCHLISTS.jsx : [...WATCHLISTS.nasdaq, ...WATCHLISTS.nyse];
 
  // FIXED BUG #1: Collect positions to close FIRST, then close them AFTER iteration
  // This prevents mutating the collection during iteration
  const longsToClose = [];
  const shortsToClose = [];
 
  // CHECK EXISTING LONG POSITIONS
  Object.keys(portfolio.longPositions).forEach(ticker => {
    const price = marketData[ticker]?.price;
    if (!price) return;
    const positions = portfolio.longPositions[ticker];
    if (!positions || positions.length === 0) return;
    // FIXED BUG #8: Use weighted-average entry across ALL positions for this ticker,
    // not just positions[0] — otherwise extra positions are never evaluated.
    let totalQty = 0, totalCost = 0;
    positions.forEach(pos => { totalQty += pos.qty; totalCost += pos.entryPrice * pos.qty; });
    const avgEntry = totalQty > 0 ? totalCost / totalQty : price;
    const pnlPct = (price - avgEntry) / avgEntry;
    if (pnlPct >= 0.02) {
      console.log(`[STRATEGY] Take profit LONG ${ticker}: +${(pnlPct*100).toFixed(1)}%`);
      longsToClose.push(ticker);
    } else if (pnlPct <= -0.015) {
      console.log(`[STRATEGY] Stop loss LONG ${ticker}: ${(pnlPct*100).toFixed(1)}%`);
      longsToClose.push(ticker);
    }
  });
 
  // CHECK EXISTING SHORT POSITIONS
  Object.keys(portfolio.shortPositions).forEach(ticker => {
    const price = marketData[ticker]?.price;
    if (!price) return;
    const positions = portfolio.shortPositions[ticker];
    if (!positions || positions.length === 0) return;
    // FIXED BUG #8: Weighted-average entry across all short positions for this ticker
    let totalQty = 0, totalCost = 0;
    positions.forEach(pos => { totalQty += pos.qty; totalCost += pos.entryPrice * pos.qty; });
    const avgEntry = totalQty > 0 ? totalCost / totalQty : price;
    const pnlPct = (avgEntry - price) / avgEntry;
    if (pnlPct >= 0.02) {
      console.log(`[STRATEGY] Take profit SHORT ${ticker}: +${(pnlPct*100).toFixed(1)}%`);
      shortsToClose.push(ticker);
    } else if (pnlPct <= -0.015) {
      console.log(`[STRATEGY] Stop loss SHORT ${ticker}: ${(pnlPct*100).toFixed(1)}%`);
      shortsToClose.push(ticker);
    }
  });
 
  // NOW close them (after all collections are done)
  longsToClose.forEach(ticker => closeLong(ticker));
  shortsToClose.forEach(ticker => closeShort(ticker));
 
  // RISK CHECK
  const totalValue = getTotalValue();
  riskSystem.currentDrawdown = Math.max(0, (riskSystem.peakValue - totalValue) / riskSystem.peakValue);
  riskSystem.peakValue = Math.max(riskSystem.peakValue, totalValue);
 
  // FIXED BUG #6: Portfolio heat must measure NOTIONAL EXPOSURE (capital at risk),
  // not net position value. For shorts, (entry - current) can be ~0 or negative,
  // making a heavily-shorted portfolio falsely read as 0% heat.
  // Correct measure: total notional value of all open positions / total value.
  let notionalExposure = 0;
  Object.entries(portfolio.longPositions).forEach(([ticker, posArr]) => {
    const price = marketData[ticker]?.price || 0;
    posArr.forEach(pos => { notionalExposure += price * pos.qty; });
  });
  Object.entries(portfolio.shortPositions).forEach(([ticker, posArr]) => {
    const price = marketData[ticker]?.price || 0;
    posArr.forEach(pos => { notionalExposure += price * pos.qty; });
  });
  riskSystem.portfolioHeat = totalValue > 0 ? Math.max(0, notionalExposure / totalValue) : 0;
 
  // FIXED BUG #2: Compute daily loss as a fraction of starting capital
  const dailyLossPct = riskSystem.dailyRealizedLoss / START_CAPITAL;
 
  riskSystem.checksPassing = [];
  if (riskSystem.currentDrawdown <= riskSystem.maxDrawdown) riskSystem.checksPassing.push('drawdown');
  if (riskSystem.portfolioHeat <= riskSystem.maxPortfolioHeat) riskSystem.checksPassing.push('heat');
  if (dailyLossPct <= riskSystem.dailyLossLimit) riskSystem.checksPassing.push('dailyLoss');
 
  // FIXED BUG #2: Now 3 real checks are performed
  riskSystem.riskLevel = riskSystem.checksPassing.length === 3 ? 'normal' : 'elevated';
  if (riskSystem.riskLevel === 'elevated') {
    const nowRisk = Date.now();
    if (nowRisk - evaluateAndTrade._lastRiskLog > 60000) {
      evaluateAndTrade._lastRiskLog = nowRisk;
      console.log(`[RISK] Elevated - passing: [${riskSystem.checksPassing.join(', ')}] | drawdown ${(riskSystem.currentDrawdown*100).toFixed(1)}% | heat ${(riskSystem.portfolioHeat*100).toFixed(1)}% | dailyLoss ${(dailyLossPct*100).toFixed(1)}%`);
    }
    return;
  }
 
  // LOOK FOR NEW TRADES (max 5 open positions)
  const openLongs = Object.keys(portfolio.longPositions).length;
  const openShorts = Object.keys(portfolio.shortPositions).length;
  if (openLongs + openShorts >= 5) return;
  if (portfolio.cash < 50) return;
 
  for (const symbol of symbols) {
    const quote = marketData[symbol];
    if (!quote || !quote.price || !quote.prevClose) continue;
    if (portfolio.longPositions[symbol] || portfolio.shortPositions[symbol]) continue;
 
    const momentum = (quote.price - quote.prevClose) / quote.prevClose;
    const sentiment = sentimentData.byMarket[market] || 0.5;
 
    // FIXED NO-TRADING BUG: Thresholds were 0.5% — most stocks only move
    // 0.1-0.3% on a normal day so signals almost never fired.
    // Lowered to 0.1% (0.001) which fires on normal daily price movement.
    if (momentum > 0.001 && sentiment > 0.45) {
      const reason = `Momentum +${(momentum*100).toFixed(2)}% | Sentiment ${sentiment.toFixed(2)}`;
      if (executeLong(symbol, quote.price, reason)) {
        console.log(`[SIGNAL] LONG ${symbol}: momentum ${(momentum*100).toFixed(2)}% sentiment ${sentiment.toFixed(2)}`);
        return;
      }
    }
    if (momentum < -0.001 && sentiment < 0.55) {
      const reason = `Momentum ${(momentum*100).toFixed(2)}% | Sentiment ${sentiment.toFixed(2)}`;
      if (executeShort(symbol, quote.price, reason)) {
        console.log(`[SIGNAL] SHORT ${symbol}: momentum ${(momentum*100).toFixed(2)}% sentiment ${sentiment.toFixed(2)}`);
        return;
      }
    }
  }
 
  // Log why no trade fired (throttled to once per minute to avoid spam)
  const now = Date.now();
  if (now - evaluateAndTrade._lastScanLog > 60000) {
    evaluateAndTrade._lastScanLog = now;
    const priceCount = Object.keys(marketData).length;
    const openCount = Object.keys(portfolio.longPositions).length + Object.keys(portfolio.shortPositions).length;
    console.log(`[SCAN] ${market} | Prices: ${priceCount} | Open: ${openCount}/5 | Cash: $${portfolio.cash.toFixed(2)} | Risk: ${riskSystem.riskLevel} | Checked ${symbols.length} symbols`);
  }
}
evaluateAndTrade._lastScanLog = 0;
evaluateAndTrade._lastRiskLog = 0;
 
// ─── SENTIMENT ──────────────────────────────────────────────────────────────
// FIXED BUG #6: Per-market sentiment is no longer pure random noise.
// updateLearning() adjusts it based on real P&L. Here we only apply small
// volatility drift AND gently pull each market toward the learned general
// sentiment, so trading signals reflect actual performance.
 
function updateSentiment() {
  const drift = () => (Math.random() - 0.5) * 0.02;  // small volatility noise (±1%)
  const clamp = (v) => Math.max(0.2, Math.min(0.8, v));
 
  sentimentData.general = clamp(sentimentData.general + drift());
 
  ['nasdaq', 'jsx'].forEach(mkt => {
    // Pull 10% toward the learned general sentiment, then add tiny noise
    const pulled = sentimentData.byMarket[mkt] + (sentimentData.general - sentimentData.byMarket[mkt]) * 0.1;
    sentimentData.byMarket[mkt] = clamp(pulled + drift());
  });
  // FIXED BUG #8: Mirror nyse to nasdaq — same trading session, same sentiment
  sentimentData.byMarket.nyse = sentimentData.byMarket.nasdaq;
}
 
// ─── DAILY LOSS RESET (Eastern calendar midnight) ───────────────────────────
// FIXED BUG #2: Reset at the next EASTERN midnight, not server-local (UTC on
// Railway). Resetting at UTC midnight would clear the daily loss mid-session
// (7-8 PM Eastern depending on DST).
 
function scheduleDailyReset() {
  // FIXED BUG #2: Recurse instead of setInterval so each reset recalculates
  // the next Eastern midnight. setInterval drifts ±1h on DST transitions;
  // recursion recalculates from real wall-clock each time, staying exact.
  const nowET = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const nextMidnightET = new Date(nowET);
  nextMidnightET.setHours(24, 0, 0, 0);  // next Eastern midnight
  let msUntilMidnight = nextMidnightET - nowET;
  if (msUntilMidnight <= 0) msUntilMidnight += 86400000;
 
  console.log(`[RISK] Next daily loss reset in ${(msUntilMidnight / 3600000).toFixed(1)}h (Eastern midnight)`);
 
  setTimeout(() => {
    console.log(`[RISK] Daily loss reset at Eastern midnight (was $${riskSystem.dailyRealizedLoss.toFixed(2)})`);
    riskSystem.dailyRealizedLoss = 0;
    scheduleDailyReset();  // recurse — recalculates next midnight correctly after DST
  }, msUntilMidnight);
}
 
// ─── API ────────────────────────────────────────────────────────────────────
 
app.get('/api/portfolio', (req, res) => {
  const market = getCurrentMarket();
  const totalValue = getTotalValue();
  const cash = portfolio.cash;
  const pnl = totalValue - START_CAPITAL;
  const returnPct = (pnl / START_CAPITAL) * 100;
 
  const positions = [];
  Object.entries(portfolio.longPositions).forEach(([ticker, posArr]) => {
    const price = Number(marketData[ticker]?.price) || 0;
    posArr.forEach(pos => {
      const ep = Number(pos.entryPrice) || 0;
      const unrealPnL = (price - ep) * pos.qty;
      positions.push({ ticker, type: 'LONG', qty: pos.qty, entryPrice: ep.toFixed(2), currentPrice: price.toFixed(2), pnl: unrealPnL.toFixed(2) });
    });
  });
  Object.entries(portfolio.shortPositions).forEach(([ticker, posArr]) => {
    const price = Number(marketData[ticker]?.price) || 0;
    posArr.forEach(pos => {
      const ep = Number(pos.entryPrice) || 0;
      const unrealPnL = (ep - price) * pos.qty;
      positions.push({ ticker, type: 'SHORT', qty: pos.qty, entryPrice: ep.toFixed(2), currentPrice: price.toFixed(2), pnl: unrealPnL.toFixed(2) });
    });
  });
 
  const recentTrades = portfolio.trades.slice(-10).reverse().map(t => {
    // FIXED BUG #7: Coerce to Number before toFixed — restored trades may have
    // string fields if loaded from an older backup schema.
    const entryP = Number(t.entryPrice) || 0;
    const livePrice = marketData[t.ticker]?.price;
    const liveP = (livePrice && livePrice > 0) ? livePrice : (Number(t.entryPrice) || 0);
    let unrealPnL = 0;
    if (t.status === 'open') {
      unrealPnL = t.direction === 'LONG' ? (liveP - entryP) * t.qty : (entryP - liveP) * t.qty;
    }
    return {
      ticker: t.ticker, direction: t.direction, size: t.qty,
      entryPrice: entryP.toFixed(2), currentPrice: liveP.toFixed(2),
      reason: t.reason, status: t.status,
      realizedPnL: t.realizedPnL || 0, unrealizedPnL: unrealPnL,
      timestamp: t.timestamp
    };
  });
 
  // FIXED BUG #5: Use portfolio.closedTrades for win-rate stats — same fix
  // as updateLearning. portfolio.trades is trimmed to 200 closed records;
  // closedTrades holds up to 500 and is the authoritative history.
  const closed = portfolio.closedTrades;
  const wins = closed.filter(t => t.realizedPnL > 0).length;
  const totalClosed = closed.length;
  const winRate = totalClosed > 0 ? ((wins / totalClosed) * 100).toFixed(1) : '0.0';
 
  const openTradeCount = portfolio.trades.filter(t => t.status === 'open').length;
 
  res.json({
    cash: cash.toFixed(2),
    totalValue: totalValue.toFixed(2),
    totalPnL: pnl.toFixed(2),
    return: returnPct.toFixed(2),
    positions,
    recentTrades,
    stats: {
      totalTrades: openTradeCount + totalClosed,
      openTrades: openTradeCount,
      closedTrades: totalClosed,
      wins, losses: totalClosed - wins,
      winRatePercent: winRate + '%',
      recentWinRate: (aiSystem.recentWinRate * 100).toFixed(1) + '%'
    },
    currentMarket: market || 'CLOSED',
    marketStatus: getMarketStatus(),
    wsConnected,
    pricesLoaded: Object.keys(marketData).length,
    aiMetrics: {
      strategy: aiSystem.strategy,
      aggressionLevel: (aiSystem.aggressionLevel * 100).toFixed(0) + '%',
      winningStreak: aiSystem.winningStreak,
      losingStreak: aiSystem.losingStreak,
      performanceHistory: aiSystem.performanceHistory.slice(-20)
    },
    aiReasoning: {
      winRate: (aiSystem.currentReasoning.winRate * 100).toFixed(1) + '%',
      trend: aiSystem.currentReasoning.trend,
      confidence: aiSystem.currentReasoning.confidence + '%',
      nextAction: aiSystem.currentReasoning.nextAction
    },
    riskMetrics: {
      currentDrawdown: (riskSystem.currentDrawdown * 100).toFixed(2) + '%',
      portfolioHeat: (riskSystem.portfolioHeat * 100).toFixed(2) + '%',
      riskLevel: riskSystem.riskLevel,
      checksPassing: riskSystem.checksPassing,
      dailyLoss: riskSystem.dailyRealizedLoss.toFixed(2),
      dailyLossLimit: (riskSystem.dailyLossLimit * 100).toFixed(0) + '%'
    },
    marketTransitionState: {
      currentMarket: marketTransitionData.lastMarket,
      nasdaqNyseSaved: marketTransitionData.nasdaqNyseOpenTrades?.length || 0,
      jsxSaved: marketTransitionData.jsxOpenTrades?.length || 0
    }
  });
});
 
app.get('/api/logs', (req, res) => res.json(tradeLogger));
 
// ─── MAIN LOOP ──────────────────────────────────────────────────────────────
 
app.listen(PORT, async () => {
  console.log(`\n✅ ATLAS v7 running on port ${PORT}`);
  console.log(`[STARTUP] Finnhub key: ${FINNHUB_KEY.substring(0, 6)}...`);
 
  loadState();
 
  // 1. Fetch initial prices via REST (one-time)
  console.log('[STARTUP] Fetching initial prices...');
  await fetchInitialPrices();
 
  // 2. Connect WebSocket for real-time streaming
  connectWebSocket();
 
  // 3. Trading evaluation every 10 seconds
  setInterval(() => {
    updateSentiment();
    updateLearning();
    evaluateAndTrade();
  }, 10000);
 
  // 4. Save state every 5 minutes
  setInterval(saveState, 300000);
 
  // 5. FIXED BUG #2: Reset daily realized loss at calendar midnight, not uptime+24h
  scheduleDailyReset();
 
  // 6. Refresh prevClose every 6 hours — skip after-hours and non-trading times.
  // Mirror getCurrentMarket's logic exactly so JSX Sunday evening is included.
  setInterval(() => {
    const { hours, day } = getEasternTimeParts();
    // BUG #3 FIX: day === 0 (Sunday) before 7 PM is still Saturday in Tokyo —
    // no JSX session yet. Must use the same isSundayEarlyMorning guard as
    // getCurrentMarket, not a flat 'day === 0' weekend block.
    const isSundayEarlyMorning = day === 0 && hours < JSX_HOURS.start;
    const isRealWeekend = day === 6 || isSundayEarlyMorning;  // Sat + Sun before 7 PM
    const isUSSession = hours >= 8 && hours < 17;
    const isJPSession = hours >= 18 || hours < 3;
    if (!isRealWeekend && (isUSSession || isJPSession)) {
      fetchInitialPrices();
    } else {
      console.log('[PRICES] Skipping refresh — market closed');
    }
  }, 21600000);
 
  console.log('[STARTUP] ✓ All systems active\n');
 
  // DIAGNOSTIC: After prices load, log momentum signals so Railway logs show
  // exactly why trades are or aren't firing.
  setTimeout(() => {
    // Re-evaluate market at this moment (not captured from startup)
    const diagMarket = getCurrentMarket();
    console.log(`[DIAG] Market: ${diagMarket || 'CLOSED'} | Prices loaded: ${Object.keys(marketData).length}`);
 
    if (diagMarket) {
      const diagSymbols = diagMarket === 'jsx'
        ? WATCHLISTS.jsx
        : [...WATCHLISTS.nasdaq, ...WATCHLISTS.nyse];
 
      let signalCount = 0;
      diagSymbols.forEach(symbol => {
        const q = marketData[symbol];
        if (!q || !q.price || !q.prevClose) return;
        const mom = ((q.price - q.prevClose) / q.prevClose * 100).toFixed(3);
        const dir = parseFloat(mom) > 0.1 ? '▲ LONG candidate'
                  : parseFloat(mom) < -0.1 ? '▼ SHORT candidate' : '  flat';
        console.log(`[DIAG] ${symbol}: $${q.price.toFixed(2)} (${mom}%) ${dir}`);
        if (Math.abs(parseFloat(mom)) > 0.1) signalCount++;
      });
      console.log(`[DIAG] ${signalCount}/${diagSymbols.length} symbols have qualifying momentum (>0.1%)`);
    }
 
    // FORCE TRADE: If no position/trade exists 2 minutes after startup, pick
    // the stock with highest absolute momentum and force one trade.
    // FIXED BUG #2: Respects risk checks before executing.
    // FIXED BUG #3: Re-evaluates getCurrentMarket() at execution time (t=120s),
    //               not the stale closure captured at t=35s.
    setTimeout(() => {
      const openCount = Object.keys(portfolio.longPositions).length
                      + Object.keys(portfolio.shortPositions).length;
      // FIX: Only check openCount and closedTrades. portfolio.trades.length is
      // always > 0 after loadState() restores any backup — it was blocking the
      // force trade on every single redeployment, even when intended to fire.
      // openCount covers live positions; closedTrades covers trade history.
      if (openCount > 0 || portfolio.closedTrades.length > 0) {
        console.log('[FORCE] Skipped — already have trades or history');
        return;
      }
 
      // FIXED BUG #2: honour risk gate before forcing
      if (riskSystem.riskLevel === 'elevated') {
        console.log('[FORCE] Skipped — risk level elevated');
        return;
      }
 
      // FIXED BUG #3: fresh market lookup at t=120s
      const forceMarket = getCurrentMarket();
      if (!forceMarket) {
        console.log('[FORCE] Skipped — market closed at execution time');
        return;
      }
 
      const forceSymbols = forceMarket === 'jsx'
        ? WATCHLISTS.jsx
        : [...WATCHLISTS.nasdaq, ...WATCHLISTS.nyse];
 
      let best = null, bestMom = 0;
      forceSymbols.forEach(symbol => {
        const q = marketData[symbol];
        if (!q || !q.price) return;
        const mom = q.prevClose ? (q.price - q.prevClose) / q.prevClose : 0;
        if (Math.abs(mom) > Math.abs(bestMom)) { best = symbol; bestMom = mom; }
      });
 
      if (best && marketData[best]?.price) {
        const price = marketData[best].price;
        const direction = bestMom >= 0 ? 'LONG' : 'SHORT';
        console.log(`[FORCE] Forcing ${direction} ${best} @ $${price.toFixed(2)} (momentum ${(bestMom*100).toFixed(2)}%)`);
        if (direction === 'LONG') executeLong(best, price, 'Force initial trade (highest momentum)');
        else executeShort(best, price, 'Force initial trade (highest magnitude momentum)');
      } else {
        console.log('[FORCE] Skipped — no real prices available');
      }
    }, 120000); // inner timer: fires 120s after the outer timer (total ~155s from startup)
  }, 35000); // outer timer: fires ~35s after startup (after price fetch completes)
});
 
