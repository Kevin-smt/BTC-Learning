import 'dotenv/config'
import express from 'express'
import { Sequelize } from 'sequelize'
import WebSocket from 'ws'
import { RSI, MACD, EMA } from 'technicalindicators'
import models from '../models/index.js'

const { NiftyData } = models

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
                `MACD: ${indicators.macd.macd?.toFixed(1)} | ` +
                `Signal: ${indicators.macd.signal?.toFixed(1)} | ` +
                `Hist: ${indicators.macd.histogram?.toFixed(1)}`
            )

        }
    }
}

//  INDICATORS

function computeIndicatorsForSeries(candles, timeframe) {

    const closes = candles.map(c => Number(c.close))

    let fast, slow, signal

    if (timeframe === '1m') [fast, slow, signal] = [12, 24, 9]
    if (timeframe === '15m') [fast, slow, signal] = [6, 13, 5]
    if (timeframe === '30m') [fast, slow, signal] = [5, 10, 5]

    const indicators = {}

    // RSI
    if (closes.length >= 15) {
        const rsiArr = RSI.calculate({
            period: 14,
            values: closes
        })
        indicators.rsi = Number(rsiArr.at(-1)?.toFixed(2))
    }

    // EMA 9
    if (closes.length >= 9) {
        const ema9Arr = EMA.calculate({
            period: 9,
            values: closes
        })
        indicators.ema9 = Number(ema9Arr.at(-1)?.toFixed(1))
    }

    // EMA 21
    if (closes.length >= 21) {
        const ema21Arr = EMA.calculate({
            period: 21,
            values: closes
        })
        indicators.ema21 = Number(ema21Arr.at(-1)?.toFixed(1))
    }

    // MACD
    if (closes.length >= slow + signal) {
        const macdArr = MACD.calculate({
            values: closes,
            fastPeriod: fast,
            slowPeriod: slow,
            signalPeriod: signal
        })

        const lastMacd = macdArr.at(-1)

        if (lastMacd) {
            indicators.macd = {
                macd: Number(lastMacd.MACD?.toFixed(1) ?? 0),
                signal: Number(lastMacd.signal?.toFixed(1) ?? 0),
                histogram: Number(lastMacd.histogram?.toFixed(1) ?? 0)
            }
        }
    }

    return indicators
}

async function calculateIndicators({
    script,
    timeframe,
    rows
}) {

    if (!rows || !rows.length) {
        console.log(`No candles found for ${script} (${timeframe})`)
        return
    }

    const tempSeries = []

    for (let i = 0; i < rows.length; i++) {

        tempSeries.push(rows[i])

        const indicators = computeIndicatorsForSeries(tempSeries, timeframe)

        rows[i].indicators = indicators
    }

    await NiftyData.bulkCreate(rows, { ignoreDuplicates: true })

    console.log(`Indicators calculated → ${script} (${timeframe})`)
}


function calculateLiveIndicators(tf) {

    const indicators = computeIndicatorsForSeries(store[tf], tf)

    const lastIndex = store[tf].length - 1
    store[tf][lastIndex].indicators = indicators
}

async function saveLiveCandle(tf) {
    const candle = store[tf].at(-1)

    await NiftyData.create({
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

//  BUILD 15m FROM 1m

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

    calculateLiveIndicators('15m')
    logCandle('15m', candle, candle.indicators)
    await saveLiveCandle('15m')

    await build30m()
}

//  BUILD 30m FROM 15m

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

    calculateLiveIndicators('30m')
    logCandle('30m', candle, candle.indicators)
    await saveLiveCandle('30m')
}

//  HANDLE 1m CLOSE

async function handle1mClose(candle) {
    store['1m'].push(candle)
    if (store['1m'].length > MAX_MEMORY) store['1m'].shift()

    calculateLiveIndicators('1m')

    logCandle('1m', candle, candle.indicators)
    await saveLiveCandle('1m')

    await build15m()
}

//  HISTORICAL SEED

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
        `https://api.india.delta.exchange/v2/history/candles` + `?symbol=${SYMBOL}&resolution=${resolution}&start=${start}&end=${end}`

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

    await calculateIndicators({
        script: 'BTCUSD',
        timeframe: '1m',
        fastPeriod: 12,
        slowPeriod: 24,
        signalPeriod: 9,
        rows: store['1m'].map(c => ({
            ...c,
            script: 'BTCUSD',
            timeframe: '1m'
        }))
    })

    await calculateIndicators({
        script: 'BTCUSD',
        timeframe: '15m',
        fastPeriod: 6,
        slowPeriod: 13,
        signalPeriod: 5,
        rows: store['15m'].map(c => ({
            ...c,
            script: 'BTCUSD',
            timeframe: '15m'
        }))
    })

    await calculateIndicators({
        script: 'BTCUSD',
        timeframe: '30m',
        fastPeriod: 5,
        slowPeriod: 10,
        signalPeriod: 5,
        rows: store['30m'].map(c => ({
            ...c,
            script: 'BTCUSD',
            timeframe: '30m'
        }))
    })

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

// FETCH LIVE DATA USING API

let lastProcessed1mTimestamp = null

// async function LatestClosed1m() {

//     try {

//         const candles = await fetchToday1m(5)

//         if (candles.length < 2) return

//         const closedCandle = candles.at(-1)

//         if (lastProcessed1mTimestamp === closedCandle.timestamp) {
//             return
//         }

//         lastProcessed1mTimestamp = closedCandle.timestamp

//         console.log("\n1m CLOSED →", new Date(closedCandle.timestamp).toLocaleString())

//         await handle1mClose(closedCandle)

//     } catch (err) {
//         console.error("Polling error:", err.message)
//     }
// }

// custom delay to align with candle close time

async function fetchToday1m(minutesBack = 5) {

    const now = new Date()

    const endSec = Math.floor(now.getTime() / 1000)
    const startSec = endSec - (minutesBack * 60)

    const url = `https://api.india.delta.exchange/v2/history/candles` + `?symbol=BTCUSD&resolution=1m&start=${startSec}&end=${endSec}`

    const res = await fetch(url)
    const json = await res.json()
    const arr = json.result || []

    return arr.reverse().map(c => ({
        timestamp: c.time * 1000,
        open: +c.open,
        high: +c.high,
        low: +c.low,
        close: +c.close
    }))
}

async function LatestClosed1m() {

    try {
        // calculate expected closed 1m candle timestamp
        const now = Date.now()
        const currentMinute = Math.floor(now / 60000) * 60000
        const expectedTimestamp = currentMinute - 60000

        const MAX_RETRIES = 5
        let attempt = 0

        while (attempt < MAX_RETRIES) {

            const candles = await fetchToday1m(5)

            if (!candles.length) return

            // check if expected timestamp exists
            const found = candles.find(c => c.timestamp === expectedTimestamp)

            if (found) {

                if (lastProcessed1mTimestamp === found.timestamp) {
                    return
                }

                lastProcessed1mTimestamp = found.timestamp

                console.log("\n1m CLOSED →", new Date(found.timestamp).toLocaleString())

                await handle1mClose(found)

                return
            }

            // not found yet → wait 1 second
            await new Promise(resolve => setTimeout(resolve, 1000))

            attempt++
        }

        console.log("Expected 1m candle not available yet.")

    } catch (err) {
        console.error("Polling error:", err.message)
    }
}

function startAlignedPolling() {

    const now = new Date()
    const seconds = now.getSeconds()

    const delay = (60 - seconds) * 1000 + 2000

    setTimeout(() => {

        LatestClosed1m()

        setInterval(LatestClosed1m, 60 * 1000)

    }, delay)
}

//    START ENGINE

async function start() {
    await seedHistorical()
    console.log("REST mode - waiting for 1m close...")
    startAlignedPolling()

    // startWebSocket()
}

start()

const app = express()
app.listen(4000, () => console.log('Server running on 4000'))