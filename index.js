import 'dotenv/config'
import express from 'express'
import { Sequelize } from 'sequelize'
import WebSocket from 'ws'
import { RSI, MACD } from 'technicalindicators'
import BTCIndicator from './models/BTCIndicator.js'

const sequelize = new Sequelize(
    process.env.DATABASE_NAME,
    process.env.DATABASE_USER,
    process.env.DATABASE_PASSWORD,
    {
        host: process.env.DATABASE_HOST,
        port: process.env.DATABASE_PORT,
        dialect: 'postgres',
        logging: false
    }
)

BTCIndicator.init(sequelize)

await sequelize.sync()
console.log('Database connected')

//    MEMORY STORE

const store = {
    '1m': [],
    '15m': [],
    '30m': []
}

const SYMBOL = 'BTCUSD'
const MAX_MEMORY = 500

function logCandle(tf, candle, indicators) {
    console.log(`\n${SYMBOL} | ${tf}`)
    console.log(`Time: ${new Date(candle.timestamp).toLocaleString()}`)
    console.log(`O:${candle.open} H:${candle.high} L:${candle.low} C:${candle.close}`)
    if (indicators) {
        console.log(`RSI: ${indicators.rsi?.toFixed(2)}`)
        if (indicators.macd) {
            console.log(
                `MACD: ${indicators.macd.macd?.toFixed(2)} | ` +
                `Signal: ${indicators.macd.signal?.toFixed(2)} | ` +
                `Hist: ${indicators.macd.histogram?.toFixed(2)}`
            )

        }
    }
}

//    INDICATORS
function calculateIndicators(tf) {

    const closes = store[tf].map(c => c.close)

    let settings
    if (tf === '1m') settings = [12, 24, 9]
    if (tf === '15m') settings = [6, 13, 5]
    if (tf === '30m') settings = [5, 10, 5]

    const [fast, slow, signal] = settings

    // Need enough candles for MACD
    if (closes.length < slow + signal) return

    const rsiArr = RSI.calculate({
        period: 14,
        values: closes
    })

    const macdArr = MACD.calculate({
        values: closes,
        fastPeriod: fast,
        slowPeriod: slow,
        signalPeriod: signal,
        SimpleMAOscillator: false,
        SimpleMASignal: false
    })

    const lastMacd = macdArr.length ? macdArr[macdArr.length - 1] : null
    const lastRsi = rsiArr.length ? rsiArr[rsiArr.length - 1] : null

    if (!lastMacd) return

    const lastIndex = store[tf].length - 1

    store[tf][lastIndex].indicators = {
        rsi: lastRsi,
        macd: {
            macd: lastMacd.MACD,
            signal: lastMacd.signal,
            histogram: lastMacd.histogram
        }
    }
}

async function saveLiveCandle(tf) {
    const candle = store[tf].at(-1)

    await BTCIndicator.create({
        script: SYMBOL,
        timeframe: tf,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        timestamp: new Date(candle.timestamp),
        indicators: candle.indicators
    })

    console.log(`Stored ${tf} candle`)
}

//    BUILD 15m FROM 1m

async function build15m() {
    if (store['1m'].length < 15) return

    const last15 = store['1m'].slice(-15)

    const firstMinute = new Date(last15[0].timestamp).getUTCMinutes()

    if (firstMinute % 15 !== 0) return
    const candle = {
        timestamp: last15[0].timestamp,
        open: last15[0].open,
        close: last15.at(-1).close,
        high: Math.max(...last15.map(c => c.high)),
        low: Math.min(...last15.map(c => c.low))
    }

    store['15m'].push(candle)
    if (store['15m'].length > MAX_MEMORY) store['15m'].shift()

    calculateIndicators('15m')
    logCandle('15m', candle, candle.indicators)
    await saveLiveCandle('15m')

    await build30m()
}

//    BUILD 30m FROM 15m

async function build30m() {
    if (store['15m'].length < 2) return

    const last2 = store['15m'].slice(-2)

    const firstMinute = new Date(last2[0].timestamp).getUTCMinutes()

    if (firstMinute % 30 !== 0) return

    const candle = {
        timestamp: last2[0].timestamp,
        open: last2[0].open,
        close: last2[1].close,
        high: Math.max(...last2.map(c => c.high)),
        low: Math.min(...last2.map(c => c.low))
    }

    store['30m'].push(candle)
    if (store['30m'].length > MAX_MEMORY) store['30m'].shift()

    calculateIndicators('30m')
    logCandle('30m', candle, candle.indicators)
    await saveLiveCandle('30m')
}

//    HANDLE 1m CLOSE

async function handle1mClose(candle) {
    store['1m'].push(candle)
    if (store['1m'].length > MAX_MEMORY) store['1m'].shift()

    calculateIndicators('1m')

    logCandle('1m', candle, candle.indicators)
    await saveLiveCandle('1m')

    await build15m()
}

//    HISTORICAL SEED

function getLastClosedTimestamp(resolution) {

    const now = new Date()

    now.setSeconds(0)
    now.setMilliseconds(0)

    if (resolution === '1m') {
        now.setMinutes(now.getMinutes() - 1)
    }

    if (resolution === '15m') {
        const minute = now.getMinutes() - (now.getMinutes() % 15)
        now.setMinutes(minute - 15)
    }

    if (resolution === '30m') {
        const minute = now.getMinutes() - (now.getMinutes() % 30)
        now.setMinutes(minute - 30)
    }

    return Math.floor(now.getTime() / 1000)
}

async function fetchDeltaHistory(resolution, limit) {

    const end = getLastClosedTimestamp(resolution)

    const secondsPerCandle = {
        '1m': 60,
        '15m': 900,
        '30m': 1800
    }

    const start = end - (limit * secondsPerCandle[resolution])

    const url =
        `https://api.india.delta.exchange/v2/history/candles` +
        `?symbol=${SYMBOL}&resolution=${resolution}&start=${start}&end=${end}`

    const res = await fetch(url)
    const json = await res.json()
    const arr = json.result || []

    return arr.reverse().map(c => ({
        timestamp: c.time * 1000,
        open: Number(c.open),
        high: Number(c.high),
        low: Number(c.low),
        close: Number(c.close)
    }))
}

async function seedHistorical() {
    console.log('Seeding historical...')

    store['1m'] = await fetchDeltaHistory('1m', 150)
    store['15m'] = await fetchDeltaHistory('15m', 100)
    store['30m'] = await fetchDeltaHistory('30m', 100)

    calculateIndicators('1m')
    calculateIndicators('15m')
    calculateIndicators('30m')

    console.log('Historical seed complete\n')
}

//    WEBSOCKET

function startWebSocket() {

    const ws = new WebSocket('wss://socket.india.delta.exchange')

    let currentFormingCandle = null

    ws.on('open', () => {
        console.log('WebSocket connected')

        ws.send(JSON.stringify({
            type: 'subscribe',
            payload: {
                channels: [
                    { name: 'candlestick_1m', symbols: [SYMBOL] }
                ]
            }
        }))
    })

    ws.on('message', async (raw) => {

        let data

        try {
            data = JSON.parse(raw)
        } catch (err) {
            return
        }

        if (data.type !== 'candlestick_1m') return
        if (!data.candle_start_time) return

        function normalizeDeltaTimestamp(ts) {
            if (!ts) return 0;

            ts = Number(ts)

            // microseconds
            if (ts > 1e15) return Math.floor(ts / 1000);

            // milliseconds
            if (ts > 1e12) return ts;

            // seconds
            return ts * 1000;
        }

        const timestamp = normalizeDeltaTimestamp(data.candle_start_time)

        const incomingCandle = {
            timestamp,
            open: Number(data.open),
            high: Number(data.high),
            low: Number(data.low),
            close: Number(data.close)
        }

        // FIRST CANDLE (engine just started)
        if (!currentFormingCandle) {
            currentFormingCandle = incomingCandle
            return
        }

        // SAME CANDLE → update forming candle
        if (incomingCandle.timestamp === currentFormingCandle.timestamp) {
            currentFormingCandle = incomingCandle
            return
        }

        // NEW CANDLE STARTED
        // → previous candle is now CLOSED
        const closedCandle = currentFormingCandle

        currentFormingCandle = incomingCandle

        try {
            await handle1mClose(closedCandle)
        } catch (err) {
            console.error('Error processing closed candle:', err.message)
        }

    })

    ws.on('close', () => {
        console.log('WebSocket closed. Reconnecting in 5s...')
        setTimeout(startWebSocket, 5000)
    })

    ws.on('error', (err) => {
        console.error('WebSocket error:', err.message)
        ws.close()
    })
}

//    START ENGINE

async function start() {
    await seedHistorical()
    startWebSocket()
}

start()

const app = express()
app.listen(4000, () => console.log('Server running on 4000'))
