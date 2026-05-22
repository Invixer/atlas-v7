// ATLAS v3 - 24/5 Global Trading Bot with Machine Learning
// Trades NASDAQ/NYSE (US Day) + JSX (Night) with real data and adaptive strategy
 
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
require('dotenv').config();
 
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
 
// CRITICAL: Serve static files FIRST
app.use(express.json());
app.use(express.static(path.join(__dirname)));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'dashboard.html')));
app.get('/dashboard.html', (req, res) => res.sendFile(path.join(__dirname, 'dashboard.html')));
 
// Configuration
const FINNHUB_KEY = process.env.FINNHUB_KEY;
const START_CAPITAL = 1000;
const DATA_FILE = path.join(__dirname, 'trading_data.json');
const ML_FILE = path.join(__dirname, 'trading_ml.json');
const TRADES_FILE = path.join(__dirname, 'trades_all.json');
 
if (!FINNHUB_KEY || FINNHUB_KEY.includes('your_')) {
  console.error('❌ ERROR: Set FINNHUB_KEY in .env');
  process.exit(1);
}
 
// Market data
let marketData = {};
let portfolio = {
  cash: START_CAPITAL,
  positions: {},
  trades: [],
  createdAt: new Date(),
  lastUpdate: new Date()
};
 
// ML Model - LEARNS from all past trades
let mlModel = {
  // Strategy parameters that adapt based on performance
  momentumThreshold: 0.05,
  rvolThreshold: 1.5,
  takeProfitPercent: 1.5,
  stopLossPercent: 1.0,
  positionSizePercent: 50,
  
  // Learning data
  tradeHistory: [],
  performanceByTime: {},
  performanceByMarket: {
    nasdaq: { wins: 0, losses: 0, totalPnL: 0 },
    nyse: { wins: 0, losses: 0, totalPnL: 0 },
    jsx: { wins: 0, losses: 0, totalPnL: 0 }
  },
  strategyMetrics: {
    momentumWins: 0,
    momentumLosses: 0,
    breakoutWins: 0,
    breakoutLosses: 0
  }
};
 
// Market schedules (US Eastern Time)
const MARKET_SCHEDULES = {
  nasdaq: {
    name: 'NASDAQ',
    openHour: 9,
    openMin: 30,
    closeHour: 16,
    closeMin: 0,
    daysOpen: [1,2,3,4,5] // Mon-Fri
  },
  nyse: {
    name: 'NYSE',
    openHour: 9,
    openMin: 30,
    closeHour: 16,
    closeMin: 0,
    daysOpen: [1,2,3,4,5]
  },
  jsx: {
    name: 'JSX (Philippine)',
    openHour: 21, // 9 PM EST = 10 AM PHT next day
    openMin: 0,
    closeHour: 4, // 4 AM EST = 5 PM PHT
    closeMin: 0,
    daysOpen: [1,2,3,4,5]
  }
};
 
// Watchlists by market
const WATCHLISTS = {
  nasdaq: ['PLTR','SOFI','MARA','HOOD','SOUN','IONQ','RKLB','BBAI','HIMS','CIFR'],
  nyse: ['F','BAC','JPM','WFC','GE','XOM','MRK','JNJ','PFE','KO'],
  jsx: ['JFC','SM','BDO','MBT','TEL','DMC','ORCM','URC','ALI','SMPH']
};
 
// Get current market
function getCurrentMarket() {
  const now = new Date();
  const estTime = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const hours = estTime.getHours();
  const mins = estTime.getMinutes();
  const day = estTime.getDay();
  
  // Check if within market hours
  if (day >= 1 && day <= 5) { // Mon-Fri
    if (hours >= 9 && (hours < 16 || (hours === 16 && mins === 0))) {
      return 'nasdaq'; // Could be NYSE too, using NASDAQ as primary
    } else if (hours >= 21 || hours < 4) {
      return 'jsx'; // JSX trading hours
    }
  }
  
  return null; // No market open
}
 
// Initialize market data
function initializeMarketData() {
  Object.entries(WATCHLISTS).forEach(([market, tickers]) => {
    tickers.forEach(ticker => {
      const key = `${market}:${ticker}`;
      marketData[key] = {
        market: market,
        ticker: ticker,
        price: 0,
        priceHistory: [],
        lastUpdate: Date.now()
      };
    });
  });
}
 
// Load all data
function loadData() {
  if (fs.existsSync(DATA_FILE)) {
    try {
      portfolio = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
      console.log(`[DATA] Loaded portfolio: $${portfolio.cash.toFixed(2)}`);
    } catch (e) {
      console.log('[DATA] Starting fresh portfolio');
    }
  }
  
  if (fs.existsSync(ML_FILE)) {
    try {
      mlModel = JSON.parse(fs.readFileSync(ML_FILE, 'utf8'));
      console.log(`[ML] Loaded ML model with ${mlModel.tradeHistory.length} historical trades`);
    } catch (e) {
      console.log('[ML] Starting fresh ML model');
    }
  }
}
 
// Save all data
function saveData() {
  fs.writeFileSync(DATA_FILE, JSON.stringify(portfolio, null, 2));
  fs.writeFileSync(ML_FILE, JSON.stringify(mlModel, null, 2));
  fs.writeFileSync(TRADES_FILE, JSON.stringify(portfolio.trades, null, 2));
}
 
setInterval(saveData, 5000);
 
// Connect to Finnhub WebSocket
function connectFinnhub() {
  const ws = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_KEY}`);
  
  ws.on('open', () => {
    console.log('[FINNHUB] 🟢 Connected');
    
    // Subscribe to all tickers
    Object.values(WATCHLISTS).flat().forEach(ticker => {
      ws.send(JSON.stringify({ type: 'subscribe', symbol: ticker }));
    });
    
    broadcastStatus('CONNECTED');
  });
  
  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      
      if (msg.type === 'trade' && msg.data) {
        msg.data.forEach(trade => {
          const ticker = trade.s;
          const market = getCurrentMarket();
          
          if (market) {
            const key = `${market}:${ticker}`;
            if (marketData[key]) {
              marketData[key].price = trade.p;
              marketData[key].lastUpdate = Date.now();
              marketData[key].priceHistory.push({
                price: trade.p,
                timestamp: trade.t || Date.now()
              });
              
              if (marketData[key].priceHistory.length > 100) {
                marketData[key].priceHistory.shift();
              }
            }
          }
        });
        
        evaluateStrategy();
        checkExits();
        broadcastPortfolioUpdate();
      }
    } catch (e) {
      console.error('[FINNHUB] Parse error:', e.message);
    }
  });
  
  ws.on('error', (err) => {
    console.error('[FINNHUB] Error:', err.message);
    broadcastStatus('ERROR');
  });
  
  ws.on('close', () => {
    console.log('[FINNHUB] Reconnecting...');
    broadcastStatus('DISCONNECTED');
    setTimeout(connectFinnhub, 5000);
  });
}
 
// Evaluate strategy - learns from past performance
function evaluateStrategy() {
  const market = getCurrentMarket();
  if (!market || portfolio.cash < 20) return;
  
  // Get relevant watchlist for current market
  const tickers = WATCHLISTS[market];
  const marketPerf = mlModel.performanceByMarket[market];
  
  tickers.forEach(ticker => {
    const key = `${market}:${ticker}`;
    const data = marketData[key];
    
    if (!data || data.price === 0 || data.priceHistory.length < 2) return;
    if (portfolio.positions[key]) return; // Already holding
    
    const history = data.priceHistory;
    const currentPrice = data.price;
    const prevPrice = history[Math.max(0, history.length - 2)].price;
    const momentum = ((currentPrice - prevPrice) / prevPrice) * 100;
    
    // Adaptive thresholds based on market performance
    let momentumThreshold = mlModel.momentumThreshold;
    let rvolMultiplier = 1;
    
    if (marketPerf.wins > 0) {
      const winRate = marketPerf.wins / (marketPerf.wins + marketPerf.losses);
      if (winRate > 0.55) {
        momentumThreshold *= 0.9; // Lower threshold - strategy working
        rvolMultiplier = 1.1;
      } else if (winRate < 0.45) {
        momentumThreshold *= 1.1; // Raise threshold - strategy struggling
        rvolMultiplier = 0.9;
      }
    }
    
    // ENTRY: Momentum-based
    const hasSignal = Math.abs(momentum) > momentumThreshold;
    const goodPrice = currentPrice < 100; // Adjust per market
    const haveCapital = portfolio.cash > currentPrice;
    
    if (hasSignal && goodPrice && haveCapital) {
      executeTrade({
        market: market,
        ticker: ticker,
        price: currentPrice,
        momentum: momentum,
        strategy: Math.abs(momentum) > 0.1 ? 'momentum' : 'breakout'
      });
    }
  });
}
 
// Execute trade
function executeTrade(setup) {
  const { market, ticker, price, momentum, strategy } = setup;
  const key = `${market}:${ticker}`;
  
  const qty = Math.floor(portfolio.cash * (mlModel.positionSizePercent / 100) / price);
  if (qty < 1) return;
  
  const cost = qty * price;
  portfolio.cash -= cost;
  
  if (!portfolio.positions[key]) {
    portfolio.positions[key] = [];
  }
  
  portfolio.positions[key].push({
    qty: qty,
    entryPrice: price,
    entryTime: Date.now(),
    momentum: momentum,
    strategy: strategy,
    id: `${key}_${Date.now()}`
  });
  
  const trade = {
    type: 'BUY',
    market: market,
    ticker: ticker,
    qty: qty,
    price: price,
    timestamp: new Date().toISOString(),
    momentum: momentum,
    strategy: strategy
  };
  
  portfolio.trades.push(trade);
  mlModel.tradeHistory.push(trade);
  
  console.log(`[${market.toUpperCase()}] BUY ${qty}x ${ticker} @$${price.toFixed(2)} | Momentum: ${momentum.toFixed(3)}%`);
  broadcastTrade(trade);
}
 
// Check exits
function checkExits() {
  Object.entries(portfolio.positions).forEach(([key, posArray]) => {
    const [market, ticker] = key.split(':');
    const currentPrice = marketData[key].price;
    
    if (currentPrice === 0 || !Array.isArray(posArray)) return;
    
    portfolio.positions[key] = posArray.filter(pos => {
      const pnl = (currentPrice - pos.entryPrice) / pos.entryPrice;
      const pnlDollars = (currentPrice - pos.entryPrice) * pos.qty;
      
      const shouldTP = pnl >= (mlModel.takeProfitPercent / 100);
      const shouldSL = pnl <= -(mlModel.stopLossPercent / 100);
      
      if (shouldTP || shouldSL) {
        closeTrade(key, currentPrice, pnlDollars, shouldTP, pos, market);
        return false;
      }
      
      return true;
    });
    
    if (portfolio.positions[key].length === 0) {
      delete portfolio.positions[key];
    }
  });
}
 
// Close trade and update ML model
function closeTrade(key, exitPrice, pnlDollars, isProfit, pos, market) {
  const [, ticker] = key.split(':');
  portfolio.cash += exitPrice * pos.qty;
  
  const trade = {
    type: 'SELL',
    market: market,
    ticker: ticker,
    qty: pos.qty,
    price: exitPrice,
    entryPrice: pos.entryPrice,
    pnl: pnlDollars,
    timestamp: new Date().toISOString(),
    strategy: pos.strategy,
    reason: isProfit ? 'TARGET' : 'LOSS'
  };
  
  portfolio.trades.push(trade);
  mlModel.tradeHistory.push(trade);
  
  // Update market performance
  const perf = mlModel.performanceByMarket[market];
  if (isProfit) {
    perf.wins++;
  } else {
    perf.losses++;
  }
  perf.totalPnL += pnlDollars;
  
  // Update strategy metrics
  if (pos.strategy === 'momentum') {
    if (isProfit) {
      mlModel.strategyMetrics.momentumWins++;
    } else {
      mlModel.strategyMetrics.momentumLosses++;
    }
  } else {
    if (isProfit) {
      mlModel.strategyMetrics.breakoutWins++;
    } else {
      mlModel.strategyMetrics.breakoutLosses++;
    }
  }
  
  const action = isProfit ? 'PROFIT' : 'LOSS';
  console.log(`[${action}] SELL ${pos.qty}x ${ticker} @$${exitPrice.toFixed(2)} | P&L: $${pnlDollars.toFixed(2)}`);
  broadcastTrade(trade);
  
  // Rebalance strategy if needed
  rebalanceStrategy();
}
 
// Rebalance strategy based on performance
function rebalanceStrategy() {
  const totalTrades = mlModel.strategyMetrics.momentumWins + 
                      mlModel.strategyMetrics.momentumLosses +
                      mlModel.strategyMetrics.breakoutWins +
                      mlModel.strategyMetrics.breakoutLosses;
  
  if (totalTrades > 20) {
    const momentumWR = mlModel.strategyMetrics.momentumWins / 
                       Math.max(1, mlModel.strategyMetrics.momentumWins + mlModel.strategyMetrics.momentumLosses);
    const breakoutWR = mlModel.strategyMetrics.breakoutWins / 
                       Math.max(1, mlModel.strategyMetrics.breakoutWins + mlModel.strategyMetrics.breakoutLosses);
    
    // Adjust thresholds based on what's working
    if (momentumWR > 0.6) {
      mlModel.momentumThreshold = Math.max(0.01, mlModel.momentumThreshold - 0.01);
    } else if (momentumWR < 0.4) {
      mlModel.momentumThreshold = Math.min(1, mlModel.momentumThreshold + 0.02);
    }
    
    // Adjust position sizing
    const overallWR = (mlModel.strategyMetrics.momentumWins + mlModel.strategyMetrics.breakoutWins) / totalTrades;
    if (overallWR > 0.55) {
      mlModel.positionSizePercent = Math.min(70, mlModel.positionSizePercent + 2);
    } else if (overallWR < 0.40) {
      mlModel.positionSizePercent = Math.max(30, mlModel.positionSizePercent - 2);
    }
  }
}
 
// Get portfolio metrics
function getPortfolioMetrics() {
  let unrealizedPnL = 0;
  const positionDetails = [];
  
  Object.entries(portfolio.positions).forEach(([key, posArray]) => {
    const [market, ticker] = key.split(':');
    if (!Array.isArray(posArray)) return;
    
    posArray.forEach((pos, idx) => {
      const currentPrice = marketData[key].price || pos.entryPrice;
      const positionPnL = (currentPrice - pos.entryPrice) * pos.qty;
      unrealizedPnL += positionPnL;
      
      positionDetails.push({
        market: market.toUpperCase(),
        ticker: ticker,
        qty: pos.qty,
        entryPrice: pos.entryPrice.toFixed(2),
        currentPrice: currentPrice.toFixed(2),
        unrealizedPnL: positionPnL.toFixed(2),
        return: (((currentPrice - pos.entryPrice) / pos.entryPrice) * 100).toFixed(2)
      });
    });
  });
  
  const closes = portfolio.trades.filter(t => t.type === 'SELL');
  const realizedPnL = closes.reduce((sum, t) => sum + (t.pnl || 0), 0);
  const totalPnL = realizedPnL + unrealizedPnL;
  
  const totalValue = portfolio.cash + Object.entries(portfolio.positions).reduce((sum, [key, posArray]) => {
    const currentPrice = marketData[key].price;
    if (Array.isArray(posArray)) {
      return sum + posArray.reduce((s, pos) => s + ((currentPrice || pos.entryPrice) * pos.qty), 0);
    }
    return sum;
  }, 0);
  
  const wins = closes.filter(t => t.pnl > 0).length;
  const losses = closes.filter(t => t.pnl < 0).length;
  const winRate = wins / Math.max(1, wins + losses) * 100;
  
  return {
    cash: portfolio.cash.toFixed(2),
    totalValue: totalValue.toFixed(2),
    realizedPnL: realizedPnL.toFixed(2),
    unrealizedPnL: unrealizedPnL.toFixed(2),
    totalPnL: totalPnL.toFixed(2),
    return: ((totalPnL / START_CAPITAL) * 100).toFixed(2),
    positions: positionDetails,
    trades: portfolio.trades.slice(-50),
    stats: {
      totalTrades: closes.length,
      wins: wins,
      losses: losses,
      winRate: winRate.toFixed(2),
      activePositions: Object.keys(portfolio.positions).length,
      currentMarket: getCurrentMarket() || 'CLOSED'
    },
    mlMetrics: {
      momentumThreshold: mlModel.momentumThreshold.toFixed(2),
      stopLoss: mlModel.stopLossPercent.toFixed(2),
      takeProfit: mlModel.takeProfitPercent.toFixed(2),
      positionSize: mlModel.positionSizePercent.toFixed(0) + '%',
      marketPerformance: mlModel.performanceByMarket
    }
  };
}
 
// Broadcast functions
function broadcastPortfolioUpdate() {
  const metrics = getPortfolioMetrics();
  const data = JSON.stringify({
    type: 'portfolio_update',
    ...metrics,
    timestamp: new Date().toISOString()
  });
  
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  });
}
 
function broadcastTrade(trade) {
  const data = JSON.stringify({
    type: 'new_trade',
    trade: trade
  });
  
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  });
}
 
function broadcastStatus(status) {
  const data = JSON.stringify({
    type: 'status',
    status: status
  });
  
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(data);
    }
  });
}
 
// WebSocket connections
wss.on('connection', (ws) => {
  console.log('[WS] Client connected');
  
  const metrics = getPortfolioMetrics();
  ws.send(JSON.stringify({
    type: 'initial_data',
    ...metrics
  }));
  
  ws.on('close', () => {
    console.log('[WS] Client disconnected');
  });
});
 
// API endpoints
app.get('/api/portfolio', (req, res) => {
  res.json(getPortfolioMetrics());
});
 
// Intervals
setInterval(broadcastPortfolioUpdate, 500);
setInterval(evaluateStrategy, 500);
 
const PORT = process.env.PORT || 3000;
 
initializeMarketData();
loadData();
connectFinnhub();
 
server.listen(PORT, () => {
  console.log(`[ATLAS v3] 🚀 Server running on http://localhost:${PORT}`);
  console.log(`[ATLAS v3] Trading: NASDAQ/NYSE (9:30-16:00 EST) + JSX (21:00-04:00 EST)`);
  console.log(`[ATLAS v3] Starting capital: $${START_CAPITAL}`);
  console.log(`[ATLAS v3] Dashboard: http://localhost:${PORT}/dashboard.html`);
});
 
process.on('SIGINT', () => {
  console.log('[ATLAS v3] Shutting down...');
  saveData();
  process.exit(0);
});
