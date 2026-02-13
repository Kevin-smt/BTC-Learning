// Pure indicator helpers operating on arrays of normalized candles:
// candle = { time, open, high, low, close, volume }
function closes(candles) {
  return candles.map(c => Number(c.close));
}

function emaArray(values, period) {
  if (!values || values.length < period) return [];
  const k = 2 / (period + 1);
  const out = [];
  // seed with SMA of first period
  let sum = 0;
  for (let i = 0; i < period; i++) sum += values[i];
  let prev = sum / period;
  out[period - 1] = prev;
  for (let i = period; i < values.length; i++) {
    const val = values[i];
    prev = (val - prev) * k + prev;
    out[i] = prev;
  }
  return out;
}

function emaLatest(candles, period) {
  const vals = closes(candles);
  const arr = emaArray(vals, period);
  if (!arr.length) return null;
  // find last non-undefined value
  for (let i = arr.length - 1; i >= 0; i--) {
    if (arr[i] !== undefined) return arr[i];
  }
  return null;
}

function emaSlopeLatest(candles, period) {
  const vals = closes(candles);
  const arr = emaArray(vals, period);
  // find last two defined EMA values
  let lastIdx = -1;
  for (let i = arr.length - 1; i >= 0; i--) if (arr[i] !== undefined) { lastIdx = i; break; }
  if (lastIdx <= 0) return null;
  // find previous defined
  let prevIdx = -1;
  for (let i = lastIdx - 1; i >= 0; i--) if (arr[i] !== undefined) { prevIdx = i; break; }
  if (prevIdx === -1) return null;
  return arr[lastIdx] - arr[prevIdx];
}

function rsiLatest(candles, period = 14) {
  const vals = closes(candles);
  if (vals.length < period + 1) return null;
  let gains = 0;
  let losses = 0;
  for (let i = 1; i <= period; i++) {
    const diff = vals[i] - vals[i - 1];
    if (diff >= 0) gains += diff;
    else losses += Math.abs(diff);
  }
  let avgGain = gains / period;
  let avgLoss = losses / period;
  for (let i = period + 1; i < vals.length; i++) {
    const diff = vals[i] - vals[i - 1];
    const gain = diff > 0 ? diff : 0;
    const loss = diff < 0 ? Math.abs(diff) : 0;
    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period;
  }
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

function macdLatest(candles, fast = 12, slow = 26, signal = 9) {
  const vals = closes(candles);
  if (vals.length < slow + signal) return null;
  const emaFast = emaArray(vals, fast);
  const emaSlow = emaArray(vals, slow);
  const macdLine = [];
  for (let i = 0; i < vals.length; i++) {
    const f = emaFast[i];
    const s = emaSlow[i];
    if (f !== undefined && s !== undefined) macdLine[i] = f - s;
  }
  const signalArr = emaArray(macdLine.filter(v => v !== undefined), signal);
  if (!signalArr.length) return null;
  const lastMacd = macdLine[macdLine.length - 1];
  const lastSignal = signalArr[signalArr.length - 1];
  if (lastMacd === undefined || lastSignal === undefined) return null;
  return {
    macd: lastMacd,
    signal: lastSignal,
    hist: lastMacd - lastSignal
  };
}

function trendLatest(candles) {
  // Simple trend: use MACD histogram if available, else EMA(21) slope
  const macd = macdLatest(candles);
  if (macd && typeof macd.hist === 'number') {
    if (macd.hist > 0) return 'UP';
    if (macd.hist < 0) return 'DOWN';
    return 'NEUTRAL';
  }
  const slope = emaSlopeLatest(candles, 21);
  if (slope === null) return 'n/a';
  if (slope > 0) return 'UP';
  if (slope < 0) return 'DOWN';
  return 'NEUTRAL';
}

export { emaLatest, rsiLatest, macdLatest, trendLatest, emaArray };

/**
 * Compute a simple composite trading signal (BUY / SELL / NEUTRAL)
 * based on EMA crossover (12/26), MACD histogram, and RSI(14).
 * - score starts at 0
 * - EMA(12)>EMA(26) => +1 else -1
 * - MACD.hist > 0 => +1 else -1
 * - RSI > 60 => +1 ; RSI < 40 => -1
 * Interpretation:
 *  score >= 2 => BUY
 *  score <= -2 => SELL
 *  else => NEUTRAL
 * RSI extreme override: RSI < 30 => BUY, RSI > 70 => SELL
 */
function computeSignal(candles) {
  // Use EMA(9) and EMA(21) for crossover signal (better for 5m crypto)
  const ema9 = emaLatest(candles, 9);
  const ema21 = emaLatest(candles, 21);
  const macd = macdLatest(candles);
  const rsi = rsiLatest(candles, 14);

  const reasons = [];
  let score = 0;
  if (ema9 !== null && ema21 !== null) {
    if (ema9 > ema21) { score += 1; reasons.push('EMA9>EMA21'); }
    else { score -= 1; reasons.push('EMA9<EMA21'); }
  }
  if (macd && typeof macd.hist === 'number') {
    if (macd.hist > 0) { score += 1; reasons.push('MACD.hist>0'); }
    else { score -= 1; reasons.push('MACD.hist<0'); }
  }
  if (typeof rsi === 'number') {
    if (rsi > 60) { score += 1; reasons.push(`RSI=${rsi.toFixed(2)}>60`); }
    else if (rsi < 40) { score -= 1; reasons.push(`RSI=${rsi.toFixed(2)}<40`); }
    else { reasons.push(`RSI=${rsi.toFixed(2)}`); }
  }

  // RSI extremes override
  if (typeof rsi === 'number' && rsi < 30) return { signal: 'BUY', score, reasons: ['RSI extreme <30', ...reasons] };
  if (typeof rsi === 'number' && rsi > 70) return { signal: 'SELL', score, reasons: ['RSI extreme >70', ...reasons] };

  let signal = 'NEUTRAL';
  if (score >= 2) signal = 'BUY';
  else if (score <= -2) signal = 'SELL';

  return { signal, score, reasons };
}

export { computeSignal };

