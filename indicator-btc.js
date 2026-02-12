import { RSI, EMA, MACD } from 'technicalindicators'

export function calculateRSI(closes, period = 14) {
  if (closes.length < period + 1) return null
  const result = RSI.calculate({ values: closes, period })
  return result[result.length - 1] ?? null
}

export function calculateEMA(closes, period) {
  if (closes.length < period) return null
  const result = EMA.calculate({ values: closes, period })
  return result[result.length - 1] ?? null
}

export function calculateMACD(closes, [fast, slow, signal]) {
  if (closes.length < slow + signal) return null

  const result = MACD.calculate({
    values: closes,
    fastPeriod: fast,
    slowPeriod: slow,
    signalPeriod: signal,
    SimpleMAOscillator: false,
    SimpleMASignal: false
  })

  return result[result.length - 1] ?? null
}

// ----------------


export const store = {
  '1m': [],
  '15m': [],
  '30m': []
}

const LIMIT = 500

function trim(arr) {
  if (arr.length > LIMIT) arr.shift()
}

export function add1mCandle(candle) {
  store['1m'].push(candle)
  trim(store['1m'])
}

export function build15m() {
  const arr1m = store['1m']
  if (arr1m.length < 15) return null

  const last15 = arr1m.slice(-15)

  const firstTime = new Date(last15[0].timestamp)
  
  if (firstTime.getMinutes() % 15 !== 0) return null

  const candle = {
    timestamp: last15[0].timestamp,
    open: last15[0].open,
    close: last15[14].close,
    high: Math.max(...last15.map(c => c.high)),
    low: Math.min(...last15.map(c => c.low))
  }

  store['15m'].push(candle)
  trim(store['15m'])

  return candle
}

export function build30m() {
  const arr15 = store['15m']
  if (arr15.length < 2) return null

  const last2 = arr15.slice(-2)

  const firstTime = new Date(last2[0].timestamp)
  const minute = firstTime.getMinutes()

  if (minute !== 0 && minute !== 30) return null

  const candle = {
    timestamp: last2[0].timestamp,
    open: last2[0].open,
    close: last2[1].close,
    high: Math.max(...last2.map(c => c.high)),
    low: Math.min(...last2.map(c => c.low))
  }

  store['30m'].push(candle)
  trim(store['30m'])

  return candle
}


