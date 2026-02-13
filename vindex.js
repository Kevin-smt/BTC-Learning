import express from 'express';
import { emaLatest, rsiLatest, macdLatest, trendLatest, computeSignal } from './indicators.js';
import WebSocket from 'ws';

// Delta India multi-timeframe backend (console-only)
const SYMBOL = 'BTCUSD';
const TIMEFRAMES = ['1m'];
const CANDLES_REQUIRED = 500;
const PORT = process.env.PORT || 4000;

const resolutionSeconds = {
  '1m': 60
};

function formatNum(n) {
  return (n === null || n === undefined) ? 'n/a' : Number(n).toFixed(2);
}

function normalizeTimestamp(ts) {
  if (!ts) return 0;
  // if ts looks like seconds (reasonable < 1e12), convert to ms
  if (ts < 1e12) return ts * 1000;
  return ts;
}

// in-memory store per timeframe
const candlesStore = {};

async function fetchDeltaHistory(symbol = SYMBOL, resolution = '1m', limit = CANDLES_REQUIRED) {
  const now = Math.floor(Date.now() / 1000);
  const resSec = resolutionSeconds[resolution];
  if (!resSec) throw new Error(`Unsupported resolution: ${resolution}`);
  const start = now - limit * resSec;
  const end = now;
  const url = `https://api.india.delta.exchange/v2/history/candles?symbol=${symbol}&resolution=${resolution}&start=${start}&end=${end}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Delta history fetch failed: ${res.status}`);
  const data = await res.json();
  // Normalize response shapes: Delta may return an object wrapper.
  let arr = [];
  if (Array.isArray(data)) arr = data;
  else if (Array.isArray(data.result)) arr = data.result;
  else if (Array.isArray(data.candles)) arr = data.candles;
  else if (Array.isArray(data.data)) arr = data.data;
  else if (Array.isArray(data.result?.candles)) arr = data.result.candles;
  else {
    arr = data.result || data.candles || data.data || [];
  }
  arr.reverse();
  const candles = (arr || []).map(item => ({
    time: normalizeTimestamp(item.time || item.start || item.t || 0),
    open: Number(item.open),
    high: Number(item.high),
    low: Number(item.low),
    close: Number(item.close),
    volume: Number(item.volume || item.v || 0)
  }));
  return candles;
}
function normalizeDeltaTimestamp(ts) {
  if (!ts) return 0;
  // Delta sends microseconds
  if (ts > 1e15) return Math.floor(ts / 1000);
  // milliseconds
  if (ts > 1e12) return ts;
  // seconds
  return ts * 1000;
}

function formatTime(ts) {
  return new Date(ts).toLocaleString();
}

// Prevent logging the same candle more than once per timeframe
const lastLoggedCandleTime = {};
function logIndicatorsFor(resolution) {
  const candles = candlesStore[resolution];
  if (!candles || candles.length < 2) return;

  const last = candles[candles.length - 1];

  // Do NOT log the same candle twice
  if (lastLoggedCandleTime[resolution] === last.time) {
    return;
  }
  lastLoggedCandleTime[resolution] = last.time;

  console.log(`${SYMBOL} | ${resolution}`);
  console.log(`Candle Time: ${formatTime(last.time)}`);
  console.log(
    `Open: ${formatNum(last.open)}  High: ${formatNum(last.high)}  Low: ${formatNum(last.low)}`
  );
  console.log(`Close: ${formatNum(last.close)}`);

  const ema9 = emaLatest(candles, 9);
  const ema21 = emaLatest(candles, 21);
  const rsi14 = rsiLatest(candles, 14);
  const macd = macdLatest(candles);
  const trend = trendLatest(candles);
  const sig = computeSignal(candles);

  console.log(`Trend: ${trend}`);
  console.log(
    `Signal: ${sig.signal}  (score:${sig.score})  reasons: ${sig.reasons.join(', ')}`
  );
  console.log(
    `EMA(9): ${formatNum(ema9)}  EMA(21): ${formatNum(ema21)}`
  );
  console.log(`RSI(14): ${formatNum(rsi14)}`);

  if (macd) {
    console.log(
      `MACD: ${formatNum(macd.macd)}  Signal: ${formatNum(macd.signal)}  Hist: ${formatNum(macd.hist)}`
    );
  }

  console.log('-----------------------');
}

function connectDeltaWebSocket() {
  const wss = 'wss://socket.india.delta.exchange';
  const ws = new WebSocket(wss);

  ws.on('open', () => {
    const msg = {
      type: 'subscribe',
      payload: {
        channels: [
          { name: 'candlestick_1m', symbols: [SYMBOL] }
        ]
      }
    };

    ws.send(JSON.stringify(msg));
    console.log('Connected to Delta India WS, subscribed to candlestick_1m');
  });

  ws.on('message', (raw) => {
    try {
      const data = JSON.parse(raw.toString());

      // ---- Delta candlestick validation ----
      if (
        data.type !== 'candlestick_1m' ||
        !data.candle_start_time ||
        data.open === undefined ||
        data.close === undefined
      ) {
        return;
      }

      const tf = data.resolution; // "1m"
      if (!TIMEFRAMES.includes(tf)) return;

      const candleTime = normalizeDeltaTimestamp(data.candle_start_time);

      const candle = {
        time: candleTime,
        open: Number(data.open),
        high: Number(data.high),
        low: Number(data.low),
        close: Number(data.close),
        volume: Number(data.volume || 0)
      };

      const store = candlesStore[tf] || [];
      const last = store[store.length - 1];

      if (!last) {
        store.push(candle);
        candlesStore[tf] = store;
        return;
      }

      if (candle.time === last.time) {
        store[store.length - 1] = candle;
        candlesStore[tf] = store;
        return;
      }

      logIndicatorsFor(tf); 

      // push new forming candle
      store.push(candle);
      if (store.length > CANDLES_REQUIRED) store.shift();
      candlesStore[tf] = store;

    } catch (err) {
      console.error('Delta WS parse error:', err);
    }
  });

  ws.on('close', () => {
    console.log('Delta India WS closed — reconnecting in 5s');
    setTimeout(connectDeltaWebSocket, 5000);
  });

  ws.on('error', (err) => {
    console.error('Delta India WS error', err.message || err);
    ws.terminate();
  });
}

async function main() {
  // Fetch historical candles for each timeframe
  for (const tf of TIMEFRAMES) {
    const hist = await fetchDeltaHistory(SYMBOL, tf, CANDLES_REQUIRED);
    candlesStore[tf] = hist;
    console.log(`Fetched ${hist.length} historical candles for ${tf} from Delta India.`);
  }
  for (const tf of TIMEFRAMES) logIndicatorsFor(tf);
  connectDeltaWebSocket();

  const app = express();
  app.get('/', (req, res) => res.send('Delta India history + websocket indicators running (console-only).'));
  app.listen(PORT, () => {
    console.log(`index running (console-only) on port ${PORT}`);
  });
}



main().catch(err => {
  console.error(err);
  process.exit(1);
});