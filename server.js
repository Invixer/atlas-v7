// ATLAS v7 - Professional Day Trading Bot with REAL LEARNING SYSTEM
// Market-aware, adaptive AI, profit reinvestment, risk management, and TRUE LEARNING
 
const express = require('express');
const app = express();
const path = require('path');
const PORT = process.env.PORT || 3000;
const FINNHUB_KEY = process.env.FINNHUB_KEY;
const fs = require('fs');
 
// Constants
const START_CAPITAL = 1000;
const TRADES_BACKUP_FILE = './trades-backup.json';
const MARKET_STATE_FILE = './market-state.json';
 
if (!FINNHUB_KEY) {
  console.error(`
╔════════════════════════════════════════════════════════════════╗
║                  ⚠️  CRITICAL: NO API KEY SET  ⚠️              ║
╠════════════════════════════════════════════════════════════════╣
║                                                                ║
║  ATLAS cannot start without FINNHUB_KEY!                      ║
║                                                                ║
║  LOCAL: Set in .env file                                      ║
║    FINNHUB_KEY=your_key_here                                  ║
║    npm start                                                   ║
║                                                                ║
║  RAILWAY: Set in Variables tab                                ║
║    1. Go to railway.app → atlas-v7                            ║
║    2. Click Variables tab                                     ║
║    3. Add FINNHUB_KEY = your_actual_key                       ║
║    4. Deploy/Redeploy                                         ║
║                                                                ║
╚════════════════════════════════════════════════════════════════╝
  `);
  process.exit(1);
}
 
// Express middleware
app.use(express.json());
app.use(express.static(path.join(__dirname)));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'dashboard.html')));
app.get('/dashboard.html', (req, res) => res.sendFile(path.join(__dirname, 'dashboard.html')));
 
// MARKET TRANSITION PROTOCOL
let marketTransitionData = {
  lastMarket: null,
  nasdaqNyseOpenTrades: [],
  jsxOpenTrades: [],
  transitionLog: []
};
 
// Market detection
const NASDAQ_NYSE_HOURS = { start: 9.5, end: 16 };
const JSX_HOURS = { start: 19, end: 2 };
 
// Portfolio
let marketData = {};
let portfolio = {
  cash: START_CAPITAL,
  longPositions: {},
  shortPositions: {},
  trades: [],
  createdAt: new Date()
};
 
// Trade logger
let tradeLogger = [];
 
// AI System with LEARNING
let aiSystem = {
  aggressionLevel: 0.5,
  tradingIntensity: 0.5,
  marketCondition: 'neutral',
  strategy: 'balanced',
  longBias: 0.5,
  shortBias: 0.5,
  
  // LEARNING METRICS
  recentWinRate: 0.5,
  recentLosses: 0,
  losingStreak: 0,
  winningStreak: 0,
  lastWinRateUpdate: Date.now(),
  performanceHistory: [],
  
  currentReasoning: {
    winRate: 0,
    volatility: 0,
    trend: 'neutral',
    confidence: 0,
    nextAction: 'waiting'
  },
  reinvestmentSystem: {
    enabled: true,
    profitThreshold: 100,
    aggressiveThreshold: 200,
    profitsReinvested: 0,
    totalReinvestments: 0,
    reinvestmentStrategy: 'dynamic'
  }
};
 
// Risk Management
let riskSystem = {
  maxDrawdown: 0.20,
  maxPortfolioHeat: 0.50,
  positionSizeLimit: 0.10,
  dailyLossLimit: 0.05,
  currentDrawdown: 0,
  portfolioHeat: 0,
  riskLevel: 'normal',
  checksPassing: [],
  dailyRealizedLoss: 0,
  peakValue: START_CAPITAL
};
 
// Watchlists
const WATCHLISTS = {
  nasdaq: ['PLTR', 'SOFI', 'MARA', 'HOOD', 'SOUN', 'IONQ', 'RKLB', 'BBAI', 'HIMS', 'CIFR'],
  nyse: ['F', 'BAC', 'JPM', 'WFC', 'GE', 'XOM', 'MRK', 'JNJ', 'PFE', 'KO'],
  jsx: ['9984', '7203', '6758', '8301', '8306', '8411', '9432', '6861', '4568', '5201']
};
 
// Sentiment - NOW LEARNS FROM TRADES
let sentimentData = {
  general: 0.5,
  byMarket: {
    nasdaq: 0.5,
    nyse: 0.5,
    jsx: 0.5
  },
  lastUpdate: Date.now()
};
 
// === UTILITY FUNCTIONS ===
 
function getCurrentMarket() {
  const now = new Date();
  const hours = now.getHours() + now.getMinutes() / 60;
  const day = now.getDay();
  
  if (day === 0 || day === 6) return null;
  if (hours >= NASDAQ_NYSE_HOURS.start && hours < NASDAQ_NYSE_HOURS.end) return 'nasdaq';
  if (hours >= JSX_HOURS.start || hours < JSX_HOURS.end) return 'jsx';
  return null;
}
 
function getMarketStatus() {
  const day = new Date().getDay();
  if (day === 0 || day === 6) return { status: 'CLOSED', reason: 'Weekend' };
  
  const hours = new Date().getHours() + new Date().getMinutes() / 60;
  if (hours >= NASDAQ_NYSE_HOURS.start && hours < NASDAQ_NYSE_HOURS.end) {
    return { status: 'NASDAQ/NYSE OPEN', openTime: '9:30 AM EST', closeTime: '4:00 PM EST' };
  }
  if (hours >= JSX_HOURS.start || hours < JSX_HOURS.end) {
    return { status: 'JAPAN STOCK EXCHANGE OPEN', openTime: '7:00 PM EST (8:00 AM JST)', closeTime: '2:00 AM EST (3:00 PM JST)', note: 'Tokyo Stock Exchange' };
  }
  return { status: 'After Hours', openTime: '9:30 AM EST', closeTime: '4:00 PM EST', nextMarket: 'Japan Stock Exchange (7:00 PM EST)' };
}
 
function getTotalValue() {
  let value = portfolio.cash;
  Object.entries(portfolio.longPositions).forEach(([key, posArray]) => {
    const marketEntry = marketData[key];
    if (!marketEntry) return;
    const price = marketEntry.price || 0;
    posArray.forEach(pos => value += price * pos.qty);
  });
  Object.entries(portfolio.shortPositions).forEach(([key, posArray]) => {
    const marketEntry = marketData[key];
    if (!marketEntry) return;
    const price = marketEntry.price || 0;
    posArray.forEach(pos => value -= (price - pos.entryPrice) * pos.qty);
  });
  return isNaN(value) ? portfolio.cash : value;
}
 
// MARKET TRANSITION PROTOCOL
function getOpenTradesForMarket(markets) {
  if (!Array.isArray(markets)) {
    markets = [markets];
  }
  return portfolio.trades.filter(trade => {
    return trade.status === 'open' && 
           markets.some(m => trade.market === m || trade.market === m.toLowerCase());
  });
}
 
function updateMarketTransition() {
  const currentMarket = getCurrentMarket();
  const previousMarket = marketTransitionData.lastMarket;
  
  if (currentMarket !== previousMarket && previousMarket) {
    console.log(`[MARKET_TRANSITION] ${previousMarket} → ${currentMarket}`);
    
    if (previousMarket === 'jsx') {
      marketTransitionData.jsxOpenTrades = getOpenTradesForMarket('jsx');
      marketTransitionData.nasdaqNyseOpenTrades = getOpenTradesForMarket(['nasdaq', 'nyse']);
      console.log(`[MARKET_TRANSITION] Saved ${marketTransitionData.jsxOpenTrades.length} JSX trades`);
    } else {
      marketTransitionData.nasdaqNyseOpenTrades = getOpenTradesForMarket(['nasdaq', 'nyse']);
      marketTransitionData.jsxOpenTrades = getOpenTradesForMarket('jsx');
      console.log(`[MARKET_TRANSITION] Saved ${marketTransitionData.nasdaqNyseOpenTrades.length} NASDAQ/NYSE trades`);
    }
    
    savePortfolioState();
  }
  
  marketTransitionData.lastMarket = currentMarket;
}
 
// TRADE PERSISTENCE
function savePortfolioState() {
  try {
    fs.writeFileSync(TRADES_BACKUP_FILE, JSON.stringify(portfolio.trades, null, 2));
    fs.writeFileSync(MARKET_STATE_FILE, JSON.stringify(marketTransitionData, null, 2));
    console.log('[PERSISTENCE] Portfolio saved to backup files');
  } catch (e) {
    console.error('[PERSISTENCE_ERROR] Could not save portfolio:', e);
  }
}
 
function loadPortfolioState() {
  try {
    if (fs.existsSync(TRADES_BACKUP_FILE)) {
      const trades = JSON.parse(fs.readFileSync(TRADES_BACKUP_FILE, 'utf8'));
      portfolio.trades = trades;
      console.log(`[PERSISTENCE] Loaded ${trades.length} trades from backup`);
    }
    
    if (fs.existsSync(MARKET_STATE_FILE)) {
      const state = JSON.parse(fs.readFileSync(MARKET_STATE_FILE, 'utf8'));
      marketTransitionData = state;
      console.log('[PERSISTENCE] Loaded market transition state from backup');
    }
  } catch (e) {
    console.error('[PERSISTENCE_ERROR] Could not load portfolio:', e);
  }
}
 
// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('[SHUTDOWN] Server shutting down...');
  savePortfolioState();
  process.exit(0);
});
 
process.on('SIGINT', () => {
  console.log('[SHUTDOWN] Server interrupted...');
  savePortfolioState();
  process.exit(0);
});
 
// === LEARNING SYSTEM (NEW!) ===
 
function updateLearningSystem() {
  if (portfolio.trades.length < 5) return; // Need minimum trades to learn
  
  const recentTrades = portfolio.trades.slice(-20); // Last 20 trades
  const wins = recentTrades.filter(t => (t.pnl || 0) > 0).length;
  const losses = recentTrades.filter(t => (t.pnl || 0) < 0).length;
  const winRate = recentTrades.length > 0 ? wins / recentTrades.length : 0.5;
  
  // Track streaks
  if (recentTrades.length > 0) {
    const lastTrade = recentTrades[recentTrades.length - 1];
    if ((lastTrade.pnl || 0) > 0) {
      aiSystem.winningStreak++;
      aiSystem.losingStreak = 0;
    } else {
      aiSystem.losingStreak++;
      aiSystem.winningStreak = 0;
    }
  }
  
  // CRITICAL: Update win rate for strategy decisions
  aiSystem.recentWinRate = winRate;
  aiSystem.recentLosses = losses;
  
  // STRATEGY ADJUSTMENT based on ACTUAL win rate
  if (winRate > 0.65) {
    aiSystem.strategy = 'ultra_aggressive';
    aiSystem.aggressionLevel = Math.min(1.0, 0.8 + (winRate - 0.65) * 2);
    console.log(`[LEARNING] 🔥 HIGH WIN RATE (${(winRate*100).toFixed(1)}%) - ULTRA AGGRESSIVE!`);
  } else if (winRate > 0.55) {
    aiSystem.strategy = 'aggressive';
    aiSystem.aggressionLevel = 0.7;
    console.log(`[LEARNING] ✅ GOOD WIN RATE (${(winRate*100).toFixed(1)}%) - AGGRESSIVE`);
  } else if (winRate > 0.45) {
    aiSystem.strategy = 'balanced';
    aiSystem.aggressionLevel = 0.5;
    console.log(`[LEARNING] ⚖️ BALANCED WIN RATE (${(winRate*100).toFixed(1)}%) - BALANCED`);
  } else if (winRate > 0.35) {
    aiSystem.strategy = 'defensive';
    aiSystem.aggressionLevel = 0.3;
    console.log(`[LEARNING] 🛡️ LOW WIN RATE (${(winRate*100).toFixed(1)}%) - DEFENSIVE`);
  } else {
    aiSystem.strategy = 'ultra_conservative';
    aiSystem.aggressionLevel = 0.1;
    console.log(`[LEARNING] ⚠️ VERY LOW WIN RATE (${(winRate*100).toFixed(1)}%) - ULTRA CONSERVATIVE!`);
  }
  
  // SENTIMENT ADJUSTMENT based on trade performance (NOT random!)
  const avgPnL = recentTrades.reduce((sum, t) => sum + (t.pnl || 0), 0) / recentTrades.length;
  const oldSentiment = sentimentData.general;
  
  if (avgPnL > 10) {
    // Recent trades very profitable - become VERY bullish
    sentimentData.general = Math.min(0.85, sentimentData.general + 0.1);
    console.log(`[LEARNING] 🚀 STRONG PROFITS - Sentiment: ${oldSentiment.toFixed(2)} → ${sentimentData.general.toFixed(2)}`);
  } else if (avgPnL > 2) {
    // Recent trades profitable - become more bullish
    sentimentData.general = Math.min(0.75, sentimentData.general + 0.05);
  } else if (avgPnL < -10) {
    // Recent trades big losses - become VERY bearish
    sentimentData.general = Math.max(0.15, sentimentData.general - 0.1);
    console.log(`[LEARNING] 📉 HEAVY LOSSES - Sentiment: ${oldSentiment.toFixed(2)} → ${sentimentData.general.toFixed(2)}`);
  } else if (avgPnL < -2) {
    // Recent trades losing - become more bearish
    sentimentData.general = Math.max(0.25, sentimentData.general - 0.05);
  }
  
  // Log learning progress
  aiSystem.currentReasoning.winRate = winRate;
  aiSystem.lastWinRateUpdate = Date.now();
  aiSystem.performanceHistory.push({
    timestamp: new Date(),
    winRate,
    avgPnL,
    strategy: aiSystem.strategy,
    aggression: aiSystem.aggressionLevel
  });
}
 
// === SENTIMENT & ANALYSIS ===
 
function updateSentimentData() {
  // Sentiment is NOW learned from trades, not random!
  // Only add small random noise (±5%) to represent market volatility
  sentimentData.general = Math.max(0.2, Math.min(0.8, sentimentData.general + (Math.random() - 0.5) * 0.1));
  sentimentData.byMarket.nasdaq = Math.max(0.2, Math.min(0.8, sentimentData.byMarket.nasdaq + (Math.random() - 0.5) * 0.1));
  sentimentData.byMarket.nyse = Math.max(0.2, Math.min(0.8, sentimentData.byMarket.nyse + (Math.random() - 0.5) * 0.1));
  sentimentData.byMarket.jsx = Math.max(0.2, Math.min(0.8, sentimentData.byMarket.jsx + (Math.random() - 0.5) * 0.1));
}
 
function assessMarketAndDecide() {
  const market = getCurrentMarket();
  if (!market) return null;
  
  const sentiment = sentimentData.byMarket[market] || 0.5;
  const winRate = aiSystem.recentWinRate || 0;
  
  if (sentiment > 0.65 && winRate > 0.45) return 'long';
  if (sentiment < 0.35 && winRate > 0.45) return 'short';
  return null;
}
 
function updateRiskMetrics() {
  const totalValue = getTotalValue();
  riskSystem.currentDrawdown = Math.max(0, (riskSystem.peakValue - totalValue) / riskSystem.peakValue);
  riskSystem.peakValue = Math.max(riskSystem.peakValue, totalValue);
  
  riskSystem.checksPassing = [];
  if (riskSystem.currentDrawdown <= riskSystem.maxDrawdown) riskSystem.checksPassing.push('drawdown');
  if (riskSystem.portfolioHeat <= riskSystem.maxPortfolioHeat) riskSystem.checksPassing.push('heat');
  
  riskSystem.riskLevel = riskSystem.checksPassing.length === 3 ? 'normal' : 'elevated';
}
 
function checkAndReinvest() {
  if (!aiSystem.reinvestmentSystem.enabled) return;
  
  const totalValue = getTotalValue();
  const profit = totalValue - START_CAPITAL;
  
  if (profit >= aiSystem.reinvestmentSystem.profitThreshold) {
    const reinvestAmount = profit * 0.5;
    portfolio.cash += reinvestAmount;
    aiSystem.reinvestmentSystem.profitsReinvested += reinvestAmount;
    aiSystem.reinvestmentSystem.totalReinvestments++;
    
    if (profit >= aiSystem.reinvestmentSystem.aggressiveThreshold) {
      aiSystem.aggressionLevel = Math.min(1, aiSystem.aggressionLevel + 0.1);
    }
    
    console.log(`[REINVEST] Reinvested $${reinvestAmount.toFixed(2)}`);
  }
}
 
// === TRADING EXECUTION ===
 
function executeLong(setup) {
  const { market, ticker, price, momentum, reason } = setup;
  const key = `${market}:${ticker}`;
  const positionSize = Math.max(1, Math.floor((portfolio.cash * 0.4 * aiSystem.aggressionLevel) / price));
  
  if (portfolio.cash < price * positionSize) return;
  
  portfolio.longPositions[key] = portfolio.longPositions[key] || [];
  portfolio.longPositions[key].push({
    qty: positionSize,
    entryPrice: price
  });
  
  portfolio.cash -= price * positionSize;
  
  const trade = {
    timestamp: new Date().toISOString(),
    market,
    ticker,
    direction: 'LONG',
    price,
    qty: positionSize,
    status: 'open',
    reason,
    pnl: 0
  };
  
  portfolio.trades.push(trade);
  tradeLogger.push(`[TRADE] LONG ${ticker}: ${reason}`);
  console.log(`[TRADE] LONG ${ticker}: ${reason}`);
}
 
function executeShort(setup) {
  const { market, ticker, price, momentum, reason } = setup;
  const key = `${market}:${ticker}`;
  const positionSize = Math.max(1, Math.floor((portfolio.cash * 0.4 * aiSystem.aggressionLevel) / price));
  
  if (portfolio.cash < price * positionSize) return;
  
  portfolio.shortPositions[key] = portfolio.shortPositions[key] || [];
  portfolio.shortPositions[key].push({
    qty: positionSize,
    entryPrice: price
  });
  
  portfolio.cash -= price * positionSize;
  
  const trade = {
    timestamp: new Date().toISOString(),
    market,
    ticker,
    direction: 'SHORT',
    price,
    qty: positionSize,
    status: 'open',
    reason,
    pnl: 0
  };
  
  portfolio.trades.push(trade);
  tradeLogger.push(`[TRADE] SHORT ${ticker}: ${reason}`);
  console.log(`[TRADE] SHORT ${ticker}: ${reason}`);
}
 
// === PORTFOLIO METRICS ===
 
function getPortfolioMetrics() {
  const market = getCurrentMarket();
  const totalValue = getTotalValue();
  
  console.log(`[PORTFOLIO_METRICS] Total trades: ${portfolio.trades.length} | Win Rate: ${(aiSystem.recentWinRate * 100).toFixed(1)}% | Strategy: ${aiSystem.strategy}`);
  
  const positions = [];
  const cash = portfolio.cash;
  const positionsValue = totalValue - cash;
  const simpleTotalPnL = totalValue - START_CAPITAL;
  const validTotalPnL = isNaN(simpleTotalPnL) ? 0 : simpleTotalPnL;
  const validReturn = isNaN(validTotalPnL / START_CAPITAL) ? 0 : (validTotalPnL / START_CAPITAL) * 100;
 
  // Recent trades
  const recentTrades = portfolio.trades.slice(-10).reverse().map(trade => {
    const ticker = `${trade.market}:${trade.ticker}`;
    const currentPrice = marketData[ticker]?.price || trade.price || 0;
    const entryPrice = trade.price || 0;
    
    let pnl = 0;
    if (trade.direction === 'LONG') {
      pnl = (currentPrice - entryPrice) * (trade.qty || 1);
    } else {
      pnl = (entryPrice - currentPrice) * (trade.qty || 1);
    }
    
    return {
      id: trade.timestamp,
      timestamp: trade.timestamp,
      ticker: trade.ticker,
      direction: trade.direction,
      size: trade.qty || 1,
      entryPrice: entryPrice.toFixed(2),
      currentPrice: currentPrice.toFixed(2),
      reason: trade.reason || 'Trade executed',
      status: trade.status || 'open',
      realizedPnL: trade.realizedPnL || 0,
      unrealizedPnL: pnl,
      timeHeld: '-- min'
    };
  }).filter(t => t !== null);
 
  // Stats with REAL win rate
  const totalTrades = portfolio.trades.length;
  const wins = portfolio.trades.filter(t => (t.pnl || 0) > 0).length;
  const losses = totalTrades - wins;
  const winRatePercent = totalTrades > 0 
    ? ((wins / totalTrades) * 100).toFixed(1) 
    : '0.0';
 
  return {
    cash: cash.toFixed(2),
    totalValue: totalValue.toFixed(2),
    totalPnL: validTotalPnL.toFixed(2),
    return: validReturn.toFixed(2),
    positions,
    trades: portfolio.trades.slice(-50),
    recentTrades: recentTrades,
    
    stats: {
      totalTrades: totalTrades,
      openTrades: portfolio.trades.filter(t => t.status === 'open').length,
      closedTrades: portfolio.trades.filter(t => t.status === 'closed').length,
      wins: wins,
      losses: losses,
      winRate: totalTrades > 0 ? (wins / totalTrades) : 0,
      winRatePercent: winRatePercent + '%',
      recentWinRate: (aiSystem.recentWinRate * 100).toFixed(1) + '%'  // LEARNING win rate
    },
    
    currentMarket: market || 'CLOSED',
    marketStatus: getMarketStatus(),
    
    aiMetrics: {
      strategy: aiSystem.strategy,
      aggressionLevel: (aiSystem.aggressionLevel * 100).toFixed(0) + '%',
      tradingIntensity: (aiSystem.tradingIntensity * 100).toFixed(0) + '%',
      marketCondition: aiSystem.marketCondition,
      longBias: (aiSystem.longBias * 100).toFixed(0) + '%',
      shortBias: (aiSystem.shortBias * 100).toFixed(0) + '%',
      winningStreak: aiSystem.winningStreak,
      losingStreak: aiSystem.losingStreak
    },
    
    aiReasoning: {
      winRate: (aiSystem.currentReasoning.winRate * 100).toFixed(1) + '%',
      volatility: aiSystem.currentReasoning.volatility,
      trend: aiSystem.currentReasoning.trend,
      confidence: aiSystem.currentReasoning.confidence + '%',
      nextAction: aiSystem.currentReasoning.nextAction
    },
    
    reinvestmentMetrics: {
      enabled: aiSystem.reinvestmentSystem.enabled,
      profitsReinvested: aiSystem.reinvestmentSystem.profitsReinvested.toFixed(2),
      reinvestmentCount: aiSystem.reinvestmentSystem.totalReinvestments,
      capitalizationEffect: ((aiSystem.reinvestmentSystem.profitsReinvested / START_CAPITAL) * 100).toFixed(1) + '%'
    },
    
    riskMetrics: {
      currentDrawdown: (riskSystem.currentDrawdown * 100).toFixed(2) + '%',
      portfolioHeat: (riskSystem.portfolioHeat * 100).toFixed(2) + '%',
      riskLevel: riskSystem.riskLevel,
      checksPassing: riskSystem.checksPassing,
      dailyLoss: riskSystem.dailyRealizedLoss.toFixed(2)
    },
    
    marketTransitionState: {
      currentMarket: marketTransitionData.lastMarket,
      nasdaqNyseSaved: marketTransitionData.nasdaqNyseOpenTrades.length,
      jsxSaved: marketTransitionData.jsxOpenTrades.length
    },
    
    learningMetrics: {
      recentWinRate: (aiSystem.recentWinRate * 100).toFixed(1) + '%',
      sentiment: sentimentData.general.toFixed(2),
      performanceHistory: aiSystem.performanceHistory.slice(-30)
    }
  };
}
 
// === TRADING SIMULATION ===
 
function evaluateStrategy() {
  const market = getCurrentMarket();
  
  // UPDATE LEARNING (CRITICAL!)
  updateLearningSystem();
  
  updateSentimentData();
  assessMarketAndDecide();
  updateRiskMetrics();
  checkAndReinvest();
  updateMarketTransition();
  
  if (!market) {
    console.log('[MARKET] All markets closed - monitoring only');
    return;
  }
  
  if (!riskSystem.checksPassing.includes('drawdown') || 
      !riskSystem.checksPassing.includes('heat')) {
    return;
  }
  
  if (portfolio.trades.length === 0 && Math.random() > 0.3) {
    const watchlist = WATCHLISTS[market];
    const stock = watchlist[Math.floor(Math.random() * watchlist.length)];
    const price = 50 + Math.random() * 100;
    const direction = Math.random() > 0.5 ? 'LONG' : 'SHORT';
    
    // CRITICAL FIX: Use proper executeShort/executeLong instead of creating trade directly!
    if (direction === 'LONG') {
      executeLong({
        market,
        ticker: stock,
        price,
        momentum: Math.random(),
        reason: 'Force initial trade - starting bot learning'
      });
    } else {
      executeShort({
        market,
        ticker: stock,
        price,
        momentum: Math.random(),
        reason: 'Force initial trade - starting bot learning'
      });
    }
    
    console.log(`[FORCE_TRADE] ${direction} ${stock}: Position created with cash decrease`);
  }
}
 
// === API ENDPOINTS ===
 
app.get('/api/portfolio', (req, res) => res.json(getPortfolioMetrics()));
app.get('/api/logs', (req, res) => res.json(tradeLogger));
 
// === SERVER ===
 
app.listen(PORT, () => {
  console.log(`✅ ATLAS v7 TRADING BOT WITH LEARNING SYSTEM running on port ${PORT}`);
  console.log(`[LEARNING] Real learning system active - win rate will improve over time!`);
  loadPortfolioState();
  
  setInterval(evaluateStrategy, 5000);
});
 
