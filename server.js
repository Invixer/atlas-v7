// ATLAS v6 - Ultimate Adaptive Trading Bot
// Combines v4 adaptability + v5 risk management with smart dashboard intelligence
 
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
require('dotenv').config();
 
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
 
app.use(express.json());
app.use(express.static(path.join(__dirname)));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'dashboard.html')));
app.get('/dashboard.html', (req, res) => res.sendFile(path.join(__dirname, 'dashboard.html')));
 
const FINNHUB_KEY = process.env.FINNHUB_KEY;
const START_CAPITAL = 1000;
const DATA_FILE = path.join(__dirname, 'trading_data.json');
const ML_FILE = path.join(__dirname, 'trading_ml.json');
const LOG_FILE = path.join(__dirname, 'trading_log.json');
 
// Better error messaging for Railway and local deployment
if (!FINNHUB_KEY || FINNHUB_KEY.includes('your_') || FINNHUB_KEY === 'paste_your_api_key_here') {
  console.error('❌ ERROR: FINNHUB_KEY not configured!');
  console.error('');
  console.error('FOR RAILWAY USERS:');
  console.error('1. Go to Railway dashboard');
  console.error('2. Click your atlas-v7 project');
  console.error('3. Click "Variables" tab');
  console.error('4. Set FINNHUB_KEY = your_actual_api_key');
  console.error('5. Click "Redeploy"');
  console.error('');
  console.error('FOR LOCAL USERS:');
  console.error('1. Edit .env file');
  console.error('2. Set FINNHUB_KEY=your_actual_api_key');
  console.error('3. Save and restart (npm start)');
  console.error('');
  console.error('Get free API key at: https://finnhub.io');
  process.exit(1);
}
 
// Portfolio
let marketData = {};
let portfolio = {
  cash: START_CAPITAL,
  longPositions: {},
  shortPositions: {},
  trades: [],
  createdAt: new Date()
};
 
// AI System - Gets smarter and more adaptive
let aiSystem = {
  aggressionLevel: 0.5,
  tradingIntensity: 0.5,
  marketCondition: 'neutral',
  longBias: 0.6,
  shortBias: 0.4,
  strategy: 'balanced',
  
  // Current decision reasoning
  currentReasoning: {
    winRate: 0,
    volatility: 0,
    trend: 0,
    confidence: 0,
    nextAction: 'waiting'
  },
  
  // Profit reinvestment system
  reinvestmentSystem: {
    enabled: true,
    profitThreshold: 100, // Reinvest after $100 profit
    aggressiveThreshold: 200, // Ultra-aggressive reinvestment at $200
    reinvestmentStrategy: 'dynamic', // 'conservative' or 'dynamic'
    profitsReinvested: 0,
    totalReinvestments: 0,
    lastReinvestmentCheck: Date.now()
  }
};
 
// Risk System
let riskSystem = {
  maxDrawdown: 0.20,
  maxPortfolioHeat: 0.50,
  positionSizeLimit: 0.10,
  dailyLossLimit: 0.05,
  
  peakValue: START_CAPITAL,
  currentDrawdown: 0,
  portfolioHeat: 0,
  dailyRealizedLoss: 0,
  
  riskLevel: 'normal', // low, normal, high
  checksPassing: []
};
 
// Sentiment
let sentimentData = {
  general: 0.5,
  byMarket: { nasdaq: 0.5, nyse: 0.5, jsx: 0.5 },
  volatilityIndex: 20
};
 
// Trade logging
let tradeLogger = {
  allTrades: [],
  reasons: [],
  eventLog: []
};
 
// Japanese Stock Exchange (JSX/TSE) - Tokyo Stock Exchange
// Top Japanese companies - using numeric ticker codes for Finnhub API
const WATCHLISTS = {
  nasdaq: ['PLTR','SOFI','MARA','HOOD','SOUN','IONQ','RKLB','BBAI','HIMS','CIFR'],
  nyse: ['F','BAC','JPM','WFC','GE','XOM','MRK','JNJ','PFE','KO'],
  // jsx: Japanese companies (Tokyo Stock Exchange)
  jsx: [
    '9984',  // SoftBank Group - Tech (Finnhub: "9984")
    '7203',  // Toyota Motor - Auto
    '6758',  // Sony Group - Electronics
    '8301',  // Mizuho Financial - Banking
    '8306',  // SMFG - Banking
    '8411',  // MUFG Bank - Banking
    '9432',  // Nippon Telegraph & Telephone - Telecom
    '6861',  // Keyence - Sensors/Electronics
    '4568',  // Shionogi - Pharmaceutical
    '5201'   // Asahi Group - Beverage
  ]
};
 
function initializeMarketData() {
  Object.entries(WATCHLISTS).forEach(([market, tickers]) => {
    tickers.forEach(ticker => {
      const key = `${market}:${ticker}`;
      marketData[key] = {
        market, ticker, price: 0,
        priceHistory: [],
        lastUpdate: Date.now()
      };
    });
  });
}
 
function loadData() {
  try {
    if (fs.existsSync(DATA_FILE)) portfolio = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    if (fs.existsSync(ML_FILE)) {
      const ml = JSON.parse(fs.readFileSync(ML_FILE, 'utf8'));
      Object.assign(aiSystem, ml);
      Object.assign(riskSystem, ml);
    }
    if (fs.existsSync(LOG_FILE)) tradeLogger = JSON.parse(fs.readFileSync(LOG_FILE, 'utf8'));
    console.log('[LOADED] All data restored');
  } catch (e) {
    console.log('[DATA] Starting fresh');
  }
}
 
function saveData() {
  fs.writeFileSync(DATA_FILE, JSON.stringify(portfolio, null, 2));
  fs.writeFileSync(ML_FILE, JSON.stringify({...aiSystem, ...riskSystem}, null, 2));
  fs.writeFileSync(LOG_FILE, JSON.stringify(tradeLogger, null, 2));
}
 
setInterval(saveData, 5000);
 
// Update sentiment (simulated)
function updateSentimentData() {
  sentimentData.general = Math.max(0.1, Math.min(0.9, sentimentData.general + (Math.random() - 0.5) * 0.1));
  sentimentData.volatilityIndex = Math.max(10, Math.min(80, sentimentData.volatilityIndex + (Math.random() - 0.5) * 5));
  sentimentData.byMarket.nasdaq = sentimentData.general + (Math.random() - 0.5) * 0.2;
  sentimentData.byMarket.nyse = sentimentData.general + (Math.random() - 0.5) * 0.2;
  sentimentData.byMarket.jsx = sentimentData.general + (Math.random() - 0.5) * 0.2;
}
 
// Connect to Finnhub
function connectFinnhub() {
  const ws = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_KEY}`);
  
  ws.on('open', () => {
    console.log('[FINNHUB] 🟢 Connected');
    Object.values(WATCHLISTS).flat().forEach(ticker => {
      ws.send(JSON.stringify({ type: 'subscribe', symbol: ticker }));
    });
  });
  
  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      if (msg.type === 'trade' && msg.data) {
        msg.data.forEach(trade => {
          const market = getCurrentMarket();
          if (market) {
            const key = `${market}:${trade.s}`;
            if (marketData[key]) {
              marketData[key].price = trade.p;
              marketData[key].priceHistory.push({ price: trade.p, ts: Date.now() });
              if (marketData[key].priceHistory.length > 100) marketData[key].priceHistory.shift();
            }
          }
        });
        
        evaluateStrategy();
        checkExits();
        broadcastPortfolioUpdate();
      }
    } catch (e) {
      console.error('[FINNHUB] Error:', e.message);
    }
  });
  
  ws.on('error', () => {});
  ws.on('close', () => setTimeout(connectFinnhub, 5000));
}
 
function getCurrentMarket() {
  const now = new Date();
  const estTime = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const hours = estTime.getHours();
  const day = estTime.getDay();
  
  if (day >= 1 && day <= 5) {
    // NASDAQ/NYSE: 9:30 AM - 4:00 PM EST
    if (hours >= 9 && hours < 16) return 'nasdaq';
    
    // Japan Stock Exchange (JSX/TSE): Opens 7:00 PM EST (8:00 AM JST), closes 2:00 AM EST (3:00 PM JST)
    // Next day: 7:00 PM - 2:00 AM (midnight crossing)
    if (hours >= 19 || hours < 2) return 'jsx';
  }
  return null;
}
 
// Check profits and reinvest if conditions met
function checkAndReinvest() {
  if (!aiSystem.reinvestmentSystem.enabled) return;
  
  const now = Date.now();
  // Only check every 30 seconds
  if (now - aiSystem.reinvestmentSystem.lastReinvestmentCheck < 30000) return;
  
  aiSystem.reinvestmentSystem.lastReinvestmentCheck = now;
  
  // Calculate realized profits
  const closes = portfolio.trades.filter(t => t.pnl);
  const realizedPnL = closes.reduce((sum, t) => sum + (t.pnl || 0), 0);
  
  // Check if we should reinvest
  if (realizedPnL >= aiSystem.reinvestmentSystem.profitThreshold) {
    const reinvestAmount = Math.floor(realizedPnL * 0.5); // Reinvest 50% of profits
    portfolio.cash += reinvestAmount;
    
    aiSystem.reinvestmentSystem.profitsReinvested += reinvestAmount;
    aiSystem.reinvestmentSystem.totalReinvestments++;
    
    // Get more aggressive with reinvested capital
    if (realizedPnL >= aiSystem.reinvestmentSystem.aggressiveThreshold) {
      aiSystem.aggressionLevel = Math.min(1.0, aiSystem.aggressionLevel + 0.1);
      aiSystem.tradingIntensity = Math.min(1.0, aiSystem.tradingIntensity + 0.1);
      console.log(`[REINVEST] 🚀 Ultra-aggressive reinvestment! Profits: $${realizedPnL.toFixed(2)}`);
      console.log(`[REINVEST] Added $${reinvestAmount} to trading capital`);
      console.log(`[AI] Aggression increased to ${(aiSystem.aggressionLevel * 100).toFixed(0)}%`);
    } else {
      console.log(`[REINVEST] 💰 Reinvesting $${reinvestAmount} in profits`);
      console.log(`[REINVEST] Total profits reinvested: $${aiSystem.reinvestmentSystem.profitsReinvested.toFixed(2)}`);
    }
  }
}
 
// AI DECISION ENGINE - Real-time reasoning
function assessMarketAndDecide() {
  const market = getCurrentMarket();
  if (!market) {
    aiSystem.currentReasoning.nextAction = 'market_closed';
    return;
  }
  
  // Get recent trades
  const recentTrades = portfolio.trades.slice(-30);
  const wins = recentTrades.filter(t => t.pnl && t.pnl > 0).length;
  const losses = recentTrades.filter(t => t.pnl && t.pnl < 0).length;
  
  let winRate = 0;
  if (wins + losses > 0) {
    winRate = wins / (wins + losses);
  }
  
  // Assess market conditions
  const vix = sentimentData.volatilityIndex;
  const sentiment = sentimentData.byMarket[market];
  
  // DECISION LOGIC
  let strategy = 'balanced';
  let reasoning = {
    winRate: (winRate * 100).toFixed(1),
    volatility: vix.toFixed(0),
    trend: sentiment > 0.65 ? 'bullish' : sentiment < 0.35 ? 'bearish' : 'neutral',
    confidence: 0,
    nextAction: 'evaluate_signals'
  };
  
  // ULTRA AGGRESSIVE LOGIC
  if (winRate > 0.65 && vix < 25) {
    strategy = 'ultra_aggressive';
    aiSystem.aggressionLevel = Math.min(1.0, aiSystem.aggressionLevel + 0.1);
    aiSystem.tradingIntensity = Math.min(1.0, aiSystem.tradingIntensity + 0.1);
    reasoning.confidence = 95;
    reasoning.nextAction = 'AGGRESSIVE: High win rate + calm market';
  }
  // AGGRESSIVE LOGIC
  else if (winRate > 0.55 && vix < 30) {
    strategy = 'aggressive';
    aiSystem.aggressionLevel = 0.7;
    aiSystem.tradingIntensity = 0.7;
    reasoning.confidence = 85;
    reasoning.nextAction = 'AGGRESSIVE: Good performance detected';
  }
  // DEFENSIVE LOGIC
  else if (winRate < 0.45 || vix > 50) {
    strategy = 'defensive';
    aiSystem.aggressionLevel = 0.2;
    aiSystem.tradingIntensity = 0.2;
    reasoning.confidence = 80;
    reasoning.nextAction = 'DEFENSIVE: Low win rate or high volatility';
  }
  // BALANCED (DEFAULT)
  else {
    strategy = 'balanced';
    aiSystem.aggressionLevel = 0.5;
    aiSystem.tradingIntensity = 0.5;
    reasoning.confidence = 70;
    reasoning.nextAction = 'BALANCED: Normal market conditions';
  }
  
  // Sentiment-based long/short bias
  if (sentiment > 0.65) {
    aiSystem.marketCondition = 'bullish';
    aiSystem.longBias = 0.7;
    aiSystem.shortBias = 0.3;
  } else if (sentiment < 0.35) {
    aiSystem.marketCondition = 'bearish';
    aiSystem.longBias = 0.3;
    aiSystem.shortBias = 0.7;
  } else {
    aiSystem.marketCondition = 'neutral';
    aiSystem.longBias = 0.5;
    aiSystem.shortBias = 0.5;
  }
  
  aiSystem.strategy = strategy;
  aiSystem.currentReasoning = reasoning;
  
  console.log(`[AI] Strategy: ${strategy} | WR: ${winRate.toFixed(2)} | VIX: ${vix.toFixed(0)} | Confidence: ${reasoning.confidence}%`);
}
 
// Update risk metrics
function updateRiskMetrics() {
  const totalValue = getTotalValue();
  
  if (totalValue > riskSystem.peakValue) {
    riskSystem.peakValue = totalValue;
  }
  riskSystem.currentDrawdown = (riskSystem.peakValue - totalValue) / riskSystem.peakValue;
  
  // Risk level assessment
  if (riskSystem.currentDrawdown > 0.15) {
    riskSystem.riskLevel = 'high';
  } else if (riskSystem.currentDrawdown > 0.08) {
    riskSystem.riskLevel = 'normal';
  } else {
    riskSystem.riskLevel = 'low';
  }
  
  // Check what's passing
  riskSystem.checksPassing = [];
  if (riskSystem.currentDrawdown <= riskSystem.maxDrawdown) riskSystem.checksPassing.push('drawdown');
  if (riskSystem.portfolioHeat <= riskSystem.maxPortfolioHeat) riskSystem.checksPassing.push('heat');
  if (riskSystem.dailyRealizedLoss <= riskSystem.dailyLossLimit * portfolio.cash) riskSystem.checksPassing.push('daily');
}
 
function getTotalValue() {
  let value = portfolio.cash;
  Object.entries(portfolio.longPositions).forEach(([key, posArray]) => {
    const price = marketData[key].price;
    posArray.forEach(pos => value += price * pos.qty);
  });
  Object.entries(portfolio.shortPositions).forEach(([key, posArray]) => {
    const price = marketData[key].price;
    posArray.forEach(pos => value -= (price - pos.entryPrice) * pos.qty);
  });
  return value;
}
 
function evaluateStrategy() {
  const market = getCurrentMarket();
  
  // IMPORTANT: Always update sentiment, assess decisions, and check reinvestment
  // Even if market is closed, we need continuous analysis
  updateSentimentData();
  assessMarketAndDecide();
  updateRiskMetrics();
  checkAndReinvest(); // Check for profits to reinvest
  
  // If no market is open, don't execute trades but keep analyzing
  if (!market) {
    console.log('[MARKET] All markets closed - monitoring only');
    return;
  }
  
  // Check risk compliance before trading
  if (!riskSystem.checksPassing.includes('drawdown') || 
      !riskSystem.checksPassing.includes('heat') ||
      !riskSystem.checksPassing.includes('daily')) {
    return; // Don't trade if risk limits exceeded
  }
  
  const tickers = WATCHLISTS[market];
  
  tickers.forEach(ticker => {
    const key = `${market}:${ticker}`;
    const data = marketData[key];
    
    if (!data || data.price === 0 || data.priceHistory.length < 2) return;
    if (portfolio.longPositions[key] || portfolio.shortPositions[key]) return;
    
    const history = data.priceHistory;
    const currentPrice = data.price;
    const prevPrice = history[Math.max(0, history.length - 2)].price;
    const momentum = ((currentPrice - prevPrice) / prevPrice) * 100;
    
    const threshold = 0.05 / (aiSystem.aggressionLevel || 0.5);
    
    // GO LONG
    if (momentum > threshold && sentimentData.byMarket[market] > 0.55 && portfolio.cash > currentPrice * 2) {
      if (Math.random() < aiSystem.longBias) {
        executeLong({
          market, ticker, price: currentPrice, momentum,
          reason: `Momentum: ${momentum.toFixed(3)}% + Bullish sentiment`
        });
      }
    }
    
    // GO SHORT
    if (momentum < -threshold && sentimentData.byMarket[market] < 0.45 && portfolio.cash > currentPrice * 2) {
      if (Math.random() < aiSystem.shortBias) {
        executeShort({
          market, ticker, price: currentPrice, momentum,
          reason: `Negative momentum + Bearish sentiment`
        });
      }
    }
  });
}
 
function executeLong(setup) {
  const { market, ticker, price, momentum, reason } = setup;
  const key = `${market}:${ticker}`;
  const qty = Math.floor(portfolio.cash * 0.4 * aiSystem.aggressionLevel / price);
  
  if (qty < 1) return;
  
  portfolio.cash -= qty * price;
  if (!portfolio.longPositions[key]) portfolio.longPositions[key] = [];
  portfolio.longPositions[key].push({ qty, entryPrice: price, entryTime: Date.now() });
  
  const trade = { type: 'BUY_LONG', market, ticker, qty, price, timestamp: new Date().toISOString(), momentum };
  portfolio.trades.push(trade);
  tradeLogger.allTrades.push({...trade, reason});
  console.log(`[LONG] ${reason} - BUY ${qty}x ${ticker}`);
  broadcastTrade(trade);
}
 
function executeShort(setup) {
  const { market, ticker, price, momentum, reason } = setup;
  const key = `${market}:${ticker}`;
  const qty = Math.floor(portfolio.cash * 0.4 * aiSystem.aggressionLevel / price);
  
  if (qty < 1) return;
  
  portfolio.cash += qty * price;
  if (!portfolio.shortPositions[key]) portfolio.shortPositions[key] = [];
  portfolio.shortPositions[key].push({ qty, entryPrice: price, entryTime: Date.now() });
  
  const trade = { type: 'SELL_SHORT', market, ticker, qty, price, timestamp: new Date().toISOString(), momentum };
  portfolio.trades.push(trade);
  tradeLogger.allTrades.push({...trade, reason});
  console.log(`[SHORT] ${reason} - SHORT ${qty}x ${ticker}`);
  broadcastTrade(trade);
}
 
function checkExits() {
  const tp = 0.015 * (1 + aiSystem.aggressionLevel);
  const sl = 0.01;
  
  Object.entries(portfolio.longPositions).forEach(([key, posArray]) => {
    const price = marketData[key].price;
    if (price === 0) return;
    
    portfolio.longPositions[key] = posArray.filter(pos => {
      const pnl = (price - pos.entryPrice) / pos.entryPrice;
      const pnlDollars = (price - pos.entryPrice) * pos.qty;
      
      if (pnl >= tp || pnl <= -sl) {
        portfolio.cash += price * pos.qty;
        const [market, ticker] = key.split(':');
        const trade = {
          type: 'SELL_LONG', market, ticker, qty: pos.qty, price, pnl: pnlDollars
        };
        portfolio.trades.push(trade);
        tradeLogger.allTrades.push({...trade, reason: pnl >= tp ? 'Take Profit' : 'Stop Loss'});
        if (pnl < 0) riskSystem.dailyRealizedLoss += Math.abs(pnlDollars);
        broadcastTrade(trade);
        return false;
      }
      return true;
    });
    
    if (portfolio.longPositions[key].length === 0) delete portfolio.longPositions[key];
  });
  
  Object.entries(portfolio.shortPositions).forEach(([key, posArray]) => {
    const price = marketData[key].price;
    if (price === 0) return;
    
    portfolio.shortPositions[key] = posArray.filter(pos => {
      const pnl = (pos.entryPrice - price) / pos.entryPrice;
      const pnlDollars = (pos.entryPrice - price) * pos.qty;
      
      if (pnl >= tp || pnl <= -sl) {
        portfolio.cash -= price * pos.qty;
        const [market, ticker] = key.split(':');
        const trade = {
          type: 'COVER_SHORT', market, ticker, qty: pos.qty, price, pnl: pnlDollars
        };
        portfolio.trades.push(trade);
        tradeLogger.allTrades.push({...trade, reason: pnl >= tp ? 'Take Profit' : 'Stop Loss'});
        if (pnl < 0) riskSystem.dailyRealizedLoss += Math.abs(pnlDollars);
        broadcastTrade(trade);
        return false;
      }
      return true;
    });
    
    if (portfolio.shortPositions[key].length === 0) delete portfolio.shortPositions[key];
  });
}
 
function getPortfolioMetrics() {
  const market = getCurrentMarket();
  const totalValue = getTotalValue();
  const positions = [];
  
  Object.entries(portfolio.longPositions).forEach(([key, posArray]) => {
    const price = marketData[key].price;
    posArray.forEach(pos => {
      positions.push({
        type: 'LONG', ticker: key.split(':')[1],
        qty: pos.qty, entryPrice: pos.entryPrice.toFixed(2),
        currentPrice: price.toFixed(2),
        pnl: ((price - pos.entryPrice) * pos.qty).toFixed(2)
      });
    });
  });
  
  const closes = portfolio.trades.filter(t => t.pnl);
  const realizedPnL = closes.reduce((sum, t) => sum + (t.pnl || 0), 0);
  const unrealizedPnL = positions.reduce((sum, p) => sum + parseFloat(p.pnl), 0);
  const totalPnL = realizedPnL + unrealizedPnL;
  
  return {
    cash: portfolio.cash.toFixed(2),
    totalValue: totalValue.toFixed(2),
    realizedPnL: realizedPnL.toFixed(2),
    unrealizedPnL: unrealizedPnL.toFixed(2),
    totalPnL: totalPnL.toFixed(2),
    return: ((totalPnL / START_CAPITAL) * 100).toFixed(2),
    positions,
    trades: portfolio.trades.slice(-50),
    
    // MARKET STATUS
    currentMarket: market || 'CLOSED',
    marketStatus: getMarketStatus(),
    
    // AI METRICS - Real-time reasoning
    aiMetrics: {
      strategy: aiSystem.strategy,
      aggressionLevel: (aiSystem.aggressionLevel * 100).toFixed(0) + '%',
      tradingIntensity: (aiSystem.tradingIntensity * 100).toFixed(0) + '%',
      marketCondition: aiSystem.marketCondition,
      longBias: (aiSystem.longBias * 100).toFixed(0) + '%',
      shortBias: (aiSystem.shortBias * 100).toFixed(0) + '%'
    },
    
    // CURRENT REASONING
    aiReasoning: {
      winRate: aiSystem.currentReasoning.winRate,
      volatility: aiSystem.currentReasoning.volatility,
      trend: aiSystem.currentReasoning.trend,
      confidence: aiSystem.currentReasoning.confidence + '%',
      nextAction: aiSystem.currentReasoning.nextAction
    },
    
    // REINVESTMENT METRICS
    reinvestmentMetrics: {
      enabled: aiSystem.reinvestmentSystem.enabled,
      profitsReinvested: aiSystem.reinvestmentSystem.profitsReinvested.toFixed(2),
      reinvestmentCount: aiSystem.reinvestmentSystem.totalReinvestments,
      capitalizationEffect: ((aiSystem.reinvestmentSystem.profitsReinvested / START_CAPITAL) * 100).toFixed(1) + '%'
    },
    
    // RISK METRICS
    riskMetrics: {
      currentDrawdown: (riskSystem.currentDrawdown * 100).toFixed(2) + '%',
      portfolioHeat: (riskSystem.portfolioHeat * 100).toFixed(2) + '%',
      riskLevel: riskSystem.riskLevel,
      checksPassing: riskSystem.checksPassing,
      dailyLoss: riskSystem.dailyRealizedLoss.toFixed(2)
    }
  };
}
 
function getMarketStatus() {
  const now = new Date();
  const estTime = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const hours = estTime.getHours();
  const mins = estTime.getMinutes();
  const day = estTime.getDay();
  
  if (day === 0 || day === 6) return { status: 'CLOSED', reason: 'Weekend' };
  
  if (hours >= 9 && hours < 16) {
    return { status: 'NASDAQ/NYSE OPEN', openTime: '9:30 AM EST', closeTime: '4:00 PM EST' };
  }
  if (hours >= 16 && hours < 19) {
    return { status: 'After Hours', openTime: '9:30 AM EST', closeTime: '4:00 PM EST', nextMarket: 'Japan Stock Exchange (7:00 PM EST)' };
  }
  if (hours >= 19 || hours < 2) {
    return { status: 'JAPAN STOCK EXCHANGE OPEN', openTime: '7:00 PM EST (8:00 AM JST)', closeTime: '2:00 AM EST (3:00 PM JST)', note: 'Tokyo Stock Exchange' };
  }
  
  return { status: 'Pre-Market', openTime: '9:30 AM EST', closeTime: '4:00 PM EST' };
}
 
function broadcastPortfolioUpdate() {
  const metrics = getPortfolioMetrics();
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({ type: 'portfolio_update', ...metrics }));
    }
  });
}
 
function broadcastTrade(trade) {
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({ type: 'new_trade', trade }));
    }
  });
}
 
wss.on('connection', (ws) => {
  ws.send(JSON.stringify({ type: 'initial_data', ...getPortfolioMetrics() }));
  ws.on('close', () => {});
});
 
app.get('/api/portfolio', (req, res) => res.json(getPortfolioMetrics()));
app.get('/api/logs', (req, res) => res.json(tradeLogger));
 
setInterval(broadcastPortfolioUpdate, 500);
setInterval(evaluateStrategy, 500);
setInterval(updateSentimentData, 10000);
 
const PORT = process.env.PORT || 3000;
 
initializeMarketData();
loadData();
connectFinnhub();
 
server.listen(PORT, () => {
  console.log(`[ATLAS v6] 🚀 Ultimate Adaptive Trading Bot`);
  console.log(`[ATLAS v6] v4 Adaptability + v5 Risk Management + Smart Dashboard`);
  console.log(`[ATLAS v6] Real-time AI reasoning displayed on dashboard`);
  console.log(`[ATLAS v6] Dashboard: http://localhost:${PORT}/dashboard.html`);
});
 
process.on('SIGINT', () => {
  console.log('[ATLAS v6] Shutting down...');
  saveData();
  process.exit(0);
});
 
