import WebSocket from 'ws'
import fetch from 'node-fetch'

import {
    calculateRSI,
    calculateMACD,
    store,
    add1mCandle,
    build15m,
    build30m
} from './indicator-btc.js'

const SYMBOL = 'BTCUSD'
const HISTORY_LIMIT = 200

const MACD_CONFIG = {
    '1m': [12, 24, 9],
    '15m': [6, 13, 5],
    '30m': [5, 10, 5]
}

function logTF(tf) {
    const arr = store[tf]
    if (!arr.length) return

    const closes = arr.map(c => Number(c.close))

    const rsi = calculateRSI(closes, 14)
    const macd = calculateMACD(closes, MACD_CONFIG[tf])

    const last = arr[arr.length - 1]

    const utcDate = new Date(last.timestamp)
    const utc = utcDate.toISOString()

    const istDate = new Date(utcDate.getTime() + (5.5 * 60 * 60 * 1000))
    const ist = istDate.toISOString().replace('T', ' ').replace('Z', ' +0530')

    console.log('--------------------------------------')
    console.log(`TF: ${tf}`)
    console.log(`UTC: ${utc}`)
    console.log(`IST: ${ist}`)
    console.log(`Close: ${last.close}`)
    console.log(`RSI: ${rsi}`)
    console.log(`MACD:`, macd)
}

function normalizeTime(ts) {
    if (ts > 1e15) return Math.floor(ts / 1000)
    if (ts > 1e12) return ts
    return ts * 1000
}

async function loadHistory(tf, limit = 100) {

    const now = Math.floor(Date.now() / 1000)

    const resolutionSeconds = {
        '1m': 60,
        '15m': 900,
        '30m': 1800
    }

    const start = now - (limit * resolutionSeconds[tf])

    const url =
        `https://api.india.delta.exchange/v2/history/candles?symbol=BTCUSD&resolution=${tf}&start=${start}&end=${now}`

    const res = await fetch(url)
    const data = await res.json()

    const candles = (data.result || []).reverse()

    store[tf] = []

    candles.forEach(c => {
        store[tf].push({
            timestamp: new Date(c.time * 1000).toISOString(),
            open: Number(c.open),
            high: Number(c.high),
            low: Number(c.low),
            close: Number(c.close)
        })
    })

    console.log(`Loaded ${store[tf].length} candles for ${tf}`)
}
function startWebSocket() {

    const ws = new WebSocket('wss://socket.india.delta.exchange')

    ws.on('open', () => {
        ws.send(JSON.stringify({
            type: 'subscribe',
            payload: {
                channels: [
                    { name: 'candlestick_1m', symbols: [SYMBOL] }
                ]
            }
        }))
        console.log('Connected to Delta WS')
    })

    ws.on('message', (raw) => {

        const data = JSON.parse(raw.toString())

        if (data.type !== 'candlestick_1m') return
        if (!data.close) return

        const timestamp = new Date(
            normalizeTime(data.candle_start_time)
        ).toISOString()

        const candle = {
            timestamp,
            open: Number(data.open),
            high: Number(data.high),
            low: Number(data.low),
            close: Number(data.close)
        }

        const arr = store['1m']
        const last = arr[arr.length - 1]

        // update forming candle
        if (last && last.timestamp === timestamp) {
            arr[arr.length - 1] = candle
            return
        }

        // new closed candle
        add1mCandle(candle)

        console.log('\nNEW 1M CANDLE CLOSED')

        logTF('1m')

        const c15 = build15m()
        if (c15) {
            console.log('\nNEW 15M CANDLE BUILT')
            logTF('15m')
        }

        const c30 = build30m()
        if (c30) {
            console.log('\nNEW 30M CANDLE BUILT')
            logTF('30m')
        }
    })

    ws.on('close', () => {
        console.log('WS Closed. Reconnecting in 5s...')
        setTimeout(startWebSocket, 5000)
    })

    ws.on('error', (err) => {
        console.log('WS Error:', err.message)
    })
}



async function main() {

    console.log("Loading historical data...\n")

    await loadHistory('1m', 100)
    await loadHistory('15m', 100)
    await loadHistory('30m', 100)

    console.log("\n---- INITIAL SNAPSHOT ----\n")

    logTF('1m')
    logTF('15m')
    logTF('30m')

    console.log("\nStarting realtime engine\n")

    startWebSocket()
}

main()