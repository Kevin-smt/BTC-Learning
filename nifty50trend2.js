import tulind from 'tulind'
import moment from 'moment'
import { SmartAPI } from 'smartapi-javascript'
import { getAngelAccessToken } from './angelTokenManager.js'
import { EMA, RSI, MACD, BollingerBands } from 'technicalindicators'

const { jwtToken, refreshToken } = await getAngelAccessToken('VIBU2601')

const smart_api = new SmartAPI({
    api_key: process.env.ANGEL_API_KEY,
    access_token: jwtToken,
    refresh_token: refreshToken,
})

const momentIST = (ts) => {
    if (typeof ts === 'string' && (ts.length <= 10 || (!ts.includes('T') && !ts.includes('Z')))) {
        return moment.utc(ts).utcOffset('+05:30', true);
    }
    return moment(ts).utcOffset('+05:30');
};

function timeframeToMinutes(tf) {
    if (typeof tf === 'number') return tf
    if (!tf) return null
    const key = String(tf).toUpperCase()
    if (key === 'ONE_MINUTE' || key === '1M') return 1
    if (key === 'THREE_MINUTE' || key === '3M') return 3
    if (key === 'FIVE_MINUTE' || key === '5M') return 5
    if (key === 'FIFTEEN_MINUTE' || key === '15M') return 15
    if (key === 'THIRTY_MINUTE' || key === '30M') return 30
    if (key === 'ONE_HOUR' || key === '60M') return 60
    return null
}

function alignToMarketBucket(ts, tfMinutes) {
    const t = momentIST(ts).clone().seconds(0).milliseconds(0)
    const anchor = t.clone().startOf('day').hour(9).minute(15).second(0).millisecond(0)
    const diffMin = Math.max(0, t.diff(anchor, 'minutes'))
    const bucketIndex = Math.floor(diffMin / tfMinutes)
    return anchor.clone().add(bucketIndex * tfMinutes, 'minutes')
}

function latestEMAValue(values, period) {
    if (!period || values.length < period) return null
    const out = EMA.calculate({ period, values })
    return out.length ? out[out.length - 1] : null
}

function latestRSIValue(values, period) {
    if (!period || values.length < period + 1) return null
    const out = RSI.calculate({ period, values })
    return out.length ? out[out.length - 1] : null
}

function latestMACDValue(values, cfg) {
    if (!cfg) return null
    const { fast, slow, signal } = cfg
    if (!fast || !slow || !signal || values.length < slow + signal) return null
    const out = MACD.calculate({
        values,
        fastPeriod: fast,
        slowPeriod: slow,
        signalPeriod: signal,
        SimpleMAOscillator: false,
        SimpleMASignal: false,
    })
    return out.length ? out[out.length - 1] : null
}

function latestBBValue(values, cfg) {
    if (!cfg) return null
    const { length, stdDev } = cfg
    if (!length || !stdDev || values.length < length) return null
    const out = BollingerBands.calculate({ period: length, stdDev, values })
    return out.length ? out[out.length - 1] : null
}

function buildRollingHTFMap(baseCandles, htfMinutes, closedHtfCandles, options = {}) {
    const sortedBase = [...baseCandles].sort(
        (a, b) => momentIST(a.timestamp).valueOf() - momentIST(b.timestamp).valueOf()
    )
    const sortedClosed = [...(closedHtfCandles || [])]
        .map((c) => ({
            ...c,
            tsMs: momentIST(c.timestamp).valueOf(),
            close: Number(c.close),
        }))
        .sort((a, b) => a.tsMs - b.tsMs)
    const closedCloses = sortedClosed.map((c) => c.close)

    let ptr = 0
    let currentBucketMs = null
    let rolling = null

    const map = new Map()

    for (const candle of sortedBase) {
        const ts = momentIST(candle.timestamp)
        const bucket = alignToMarketBucket(ts, htfMinutes)
        const bucketMs = bucket.valueOf()

        if (currentBucketMs === null || bucketMs !== currentBucketMs) {
            currentBucketMs = bucketMs
            rolling = {
                timestamp: bucket.format('YYYY-MM-DD HH:mm:ss'),
                open: Number(candle.open),
                high: Number(candle.high),
                low: Number(candle.low),
                close: Number(candle.close),
            }
        } else {
            rolling.high = Math.max(rolling.high, Number(candle.high))
            rolling.low = Math.min(rolling.low, Number(candle.low))
            rolling.close = Number(candle.close)
        }

        while (ptr < sortedClosed.length && sortedClosed[ptr].tsMs < bucketMs) {
            ptr++
        }

        const series = closedCloses.slice(0, ptr)
        series.push(rolling.close)

        const emaValues = {}
        for (const [key, length] of Object.entries(options.emaPeriods || {})) {
            emaValues[key] = latestEMAValue(series, length)
        }

        const rsi = latestRSIValue(series, options.rsiPeriod)
        const macd2 = latestMACDValue(series, options.macd2Config)
        const macd3 = latestMACDValue(series, options.macd3Config)
        const bb = latestBBValue(series, options.bbConfig)

        map.set(candle.timestamp, {
            candle: { ...rolling },
            emaValues,
            rsi,
            macd2,
            macd3,
            bb,
        })
    }

    return map
}

function resampleCandles(candles, targetTimeframe) {
    if (!candles.length) return []
    const resampledCandles = []

    // Sort candles by timestamp first
    const sorted = [...candles].sort((a, b) => momentIST(a.timestamp).valueOf() - momentIST(b.timestamp).valueOf())

    let currentBucketStart = null
    let currentGroup = []

    for (const candle of sorted) {
        // Use the same 09:15-anchored logic as alignToMarketBucket
        const bucketStart = alignToMarketBucket(candle.timestamp, targetTimeframe)
        const bucketMs = bucketStart.valueOf()

        if (currentBucketStart === null || bucketMs !== currentBucketStart) {
            if (currentGroup.length > 0) {
                resampledCandles.push({
                    timestamp: momentIST(currentBucketStart).format('YYYY-MM-DD HH:mm:ss'),
                    open: currentGroup[0].open,
                    high: Math.max(...currentGroup.map((c) => c.high)),
                    low: Math.min(...currentGroup.map((c) => c.low)),
                    close: currentGroup[currentGroup.length - 1].close,
                    volume: currentGroup.reduce((sum, c) => sum + c.volume, 0),
                })
            }
            currentBucketStart = bucketMs
            currentGroup = [candle]
        } else {
            currentGroup.push(candle)
        }
    }

    // Process the last group
    if (currentGroup.length > 0) {
        resampledCandles.push({
            timestamp: momentIST(currentBucketStart).format('YYYY-MM-DD HH:mm:ss'),
            open: currentGroup[0].open,
            high: Math.max(...currentGroup.map((c) => c.high)),
            low: Math.min(...currentGroup.map((c) => c.low)),
            close: currentGroup[currentGroup.length - 1].close,
            volume: currentGroup.reduce((sum, c) => sum + currentGroup[currentGroup.length - 1].volume, 0),
        })
    }

    return resampledCandles
}

function normalizePayload(rawPayload) {
    const payload = { ...rawPayload }
    const hasDynamicTfPayload = Object.prototype.hasOwnProperty.call(payload, 'timeframe_3m_enabled')
    if (!hasDynamicTfPayload) {
        const useSupertrend = Boolean(payload.useSupertrend || payload.use_supertrend_mode)
        return {
            ...payload,
            is_dynamic_timeframe_payload: false,
            use_supertrend_mode: useSupertrend,
            rsi_enabled: useSupertrend ? false : payload.rsi_enabled,
            ema1_enabled: useSupertrend ? false : payload.ema1_enabled,
            ema2_enabled: useSupertrend ? false : payload.ema2_enabled,
            macd_enabled: useSupertrend ? false : payload.macd_enabled,
            supertrend_length: payload.supertrend_length,
            supertrend_factor: payload.supertrend_factor,
        }
    }

    const tf1Enabled = payload.timeframe_3m_enabled !== false
    const tf1 = payload.tf_timeframe_one || 'THREE_MINUTE'
    const tf1Minutes = timeframeToMinutes(tf1)
    const useSupertrend = tf1Enabled && Boolean(payload.tf_one_useSupertrend)
    const tf2Enabled = payload.timeframe_15m_enabled === true
    const tf3Enabled = payload.timeframe_30m_enabled === true
    const tf2 = payload.tf_timeframe_two || 'FIFTEEN_MINUTE'
    const tf3 = payload.tf_timeframe_three || 'THIRTY_MINUTE'
    const tf2Minutes = timeframeToMinutes(tf2)
    const tf3Minutes = timeframeToMinutes(tf3)

    const rsi2FromTf2 = tf2Enabled && Boolean(payload.tf_two_isrsi)
    const rsi2FromTf3 = tf3Enabled && Boolean(payload.tf_three_isrsi)
    const useRsi2 = rsi2FromTf2 || rsi2FromTf3

    const useMacd2 = tf2Enabled && Boolean(payload.tf_two_ismacd)
    const useMacd3 = tf3Enabled && Boolean(payload.tf_three_ismacd)

    const useEma3_15m =
        tf2Enabled &&
        tf2Minutes === 15 &&
        Boolean(payload.tf_two_isema1)
    const useEma4_15m =
        tf2Enabled &&
        tf2Minutes === 15 &&
        Boolean(payload.tf_two_isema2)

    return {
        ...payload,
        is_dynamic_timeframe_payload: true,
        candle_timeframe: tf1,
        candle_timeframe_number: tf1Minutes || 3,

        rsi_length: payload.tf_one_rsi_length,
        rsi_buy_level: payload.tf_one_rsi_buy_level,
        rsi_sell_level: payload.tf_one_rsi_sell_level,
        ema1_length: payload.tf_one_ema1,
        ema2_length: payload.tf_one_ema2,
        macd_fast: payload.tf_one_macd_fast,
        macd_slow: payload.tf_one_macd_slow,
        macd_signal: payload.tf_one_macd_signal,

        // indicator gates
        rsi_enabled: useSupertrend ? false : (tf1Enabled && Boolean(payload.tf_one_isrsi)),
        ema1_enabled: useSupertrend ? false : (tf1Enabled && Boolean(payload.tf_one_isema1)),
        ema2_enabled: useSupertrend ? false : (tf1Enabled && Boolean(payload.tf_one_isema2)),
        macd_enabled: useSupertrend ? false : (tf1Enabled && Boolean(payload.tf_one_ismacd)),

        // supertrend mode + params (3m only in this phase)
        use_supertrend_mode: useSupertrend,
        supertrend_length: payload.tf_one_supertrend_length,
        supertrend_factor: payload.tf_one_supertrend_factor,

        // keep legacy engine happy while supertrend mode ignores these
        target_points:
            typeof payload.target_points === 'number' ? payload.target_points : 1,
        stoploss_points:
            typeof payload.stoploss_points === 'number' ? payload.stoploss_points : 1,

        // map dynamic HTF payload to legacy engine keys used by /backtest
        rsi2_timeframe: useSupertrend ? null : (rsi2FromTf2 ? tf2 : (rsi2FromTf3 ? tf3 : null)),
        rsi2_length: useSupertrend ? null : (rsi2FromTf2 ? payload.tf_two_rsi_length : payload.tf_three_rsi_length),
        rsi2_buy_level: useSupertrend ? null : (rsi2FromTf2 ? payload.tf_two_rsi_buy_level : payload.tf_three_rsi_buy_level),
        rsi2_sell_level: useSupertrend ? null : (rsi2FromTf2 ? payload.tf_two_rsi_sell_level : payload.tf_three_rsi_sell_level),

        macd_2_timeframe: useSupertrend ? null : (useMacd2 ? tf2 : null),
        macd_2_fast: useSupertrend ? null : (useMacd2 ? payload.tf_two_macd_fast : null),
        macd_2_slow: useSupertrend ? null : (useMacd2 ? payload.tf_two_macd_slow : null),
        macd_2_signal: useSupertrend ? null : (useMacd2 ? payload.tf_two_macd_signal : null),

        macd_3_enabled: useSupertrend ? false : useMacd3,
        macd_3_timeframe: useSupertrend ? null : (useMacd3 ? tf3 : null),
        macd_3_fast: useSupertrend ? null : (useMacd3 ? payload.tf_three_macd_fast : null),
        macd_3_slow: useSupertrend ? null : (useMacd3 ? payload.tf_three_macd_slow : null),
        macd_3_signal: useSupertrend ? null : (useMacd3 ? payload.tf_three_macd_signal : null),

        ema3_15m_enabled: useSupertrend ? false : useEma3_15m,
        ema3_15m: useSupertrend ? null : (useEma3_15m ? payload.tf_two_ema1 : null),
        ema4_15m_enabled: useSupertrend ? false : useEma4_15m,
        ema4_15m: useSupertrend ? null : (useEma4_15m ? payload.tf_two_ema2 : null),
    }
}

function validatePayload(payload) {
    const supertrendMode = Boolean(payload.use_supertrend_mode)
    if (payload.is_dynamic_timeframe_payload && payload.timeframe_3m_enabled === false) {
        throw new Error('timeframe_3m_enabled must be true for this strategy')
    }
    if (supertrendMode && timeframeToMinutes(payload.candle_timeframe) !== 3) {
        throw new Error('Supertrend mode is currently supported only for THREE_MINUTE timeframe')
    }
    const required = [
        'instrument',
        'candle_timeframe',
        'from_date',
        'to_date',
    ]
    if (!supertrendMode) {
        required.push(
            'rsi_length',
            'rsi_buy_level',
            'rsi_sell_level',
            'ema1_length',
            'ema2_length',
            'target_points',
            'stoploss_points'
        )
    } else {
        required.push('supertrend_length', 'supertrend_factor')
    }

    const missing = required.filter((field) => !payload[field])
    if (missing.length) {
        throw new Error(`Missing required fields: ${missing.join(', ')}`)
    }

    // Validate dates
    const fromDate = momentIST(payload.from_date)
    const toDate = momentIST(payload.to_date)

    if (!fromDate.isValid() || !toDate.isValid()) {
        throw new Error('Invalid date format. Use YYYY-MM-DD')
    }

    if (fromDate.isAfter(toDate)) {
        throw new Error('from_date must be before to_date')
    }

    // Validate numeric fields
    const numericFields = ['candle_timeframe']
    if (!supertrendMode) {
        numericFields.push(
            'rsi_length',
            'rsi_buy_level',
            'rsi_sell_level',
            'ema1_length',
            'ema2_length',
            'target_points',
            'stoploss_points'
        )
    } else {
        numericFields.push('supertrend_length', 'supertrend_factor')
    }

    numericFields.forEach((field) => {
        if (
            field !== 'candle_timeframe' &&
            (typeof payload[field] !== 'number' || payload[field] <= 0)
        ) {
            throw new Error(`${field} must be a positive number`)
        }
    })

    if (!supertrendMode && payload.ema3_15m_enabled) {
        if (typeof payload.ema3_15m !== 'number' || payload.ema3_15m <= 0) {
            throw new Error('ema3_15m must be a positive number')
        }
    }
    if (!supertrendMode && payload.ema4_15m_enabled) {
        if (typeof payload.ema4_15m !== 'number' || payload.ema4_15m <= 0) {
            throw new Error('ema4_15m must be a positive number')
        }
    }

    if (!supertrendMode && payload.macd_enabled) {
        ;['macd_fast', 'macd_slow', 'macd_signal'].forEach((field) => {
            if (typeof payload[field] !== 'number' || payload[field] <= 0) {
                throw new Error(
                    `${field} must be a positive number when MACD is enabled`
                )
            }
        })

        if (payload.macd_fast >= payload.macd_slow) {
            throw new Error('macd_fast period must be less than macd_slow period')
        }

        if (payload.macd_2_timeframe) {
            ;['macd_2_fast', 'macd_2_slow', 'macd_2_signal'].forEach((field) => {
                if (typeof payload[field] !== 'number' || payload[field] <= 0) {
                    throw new Error(
                        `${field} must be a positive number when MACD_2 is enabled`
                    )
                }
            })

            if (payload.macd_2_fast >= payload.macd_2_slow) {
                throw new Error(
                    'macd_2_fast period must be less than macd_2_slow period'
                )
            }
        }
    }
    if (!supertrendMode && payload.macd_3_enabled) {

        if (payload.macd_3_timeframe) {
            ;['macd_3_fast', 'macd_3_slow', 'macd_3_signal'].forEach((field) => {
                if (typeof payload[field] !== 'number' || payload[field] <= 0) {
                    throw new Error(
                        `${field} must be a positive number when MACD_3 is enabled`
                    )
                }
            })

            if (payload.macd_3_fast >= payload.macd_3_slow) {
                throw new Error(
                    'macd_3_fast period must be less than macd_3_slow period'
                )
            }
        }
    }

    if (!supertrendMode && payload.adx_enabled) {
        const validTf = [
            'ONE_MINUTE',
            'THREE_MINUTE',
            'FIVE_MINUTE',
            'FIFTEEN_MINUTE',
            'THIRTY_MINUTE',
            'ONE_HOUR'
        ]

        if (!validTf.includes(payload.adx_timeframe)) {
            throw new Error('Invalid adx_timeframe')
        }
    }
    return true
}

function calculateWarmupPeriod(params) {
    // RSI needs rsi_length periods
    let warmup = Number(params.rsi_length) || 14

    // EMA needs roughly 2.5 times the period for proper convergence
    warmup = Math.max(warmup, Math.ceil(2.5 * (Number(params.ema1_length) || 9)))
    warmup = Math.max(warmup, Math.ceil(2.5 * (Number(params.ema2_length) || 21)))

    if (params.use_supertrend_mode) {
        warmup = Math.max(warmup, Number(params.supertrend_length) || 10)
    }

    if (params.ema3_15m_enabled) {
        warmup = Math.max(warmup, Math.ceil(2.5 * params.ema3_15m))
    }

    if (params.ema4_15m_enabled) {
        warmup = Math.max(warmup, Math.ceil(2.5 * params.ema4_15m))
    }


    // MACD needs the longer of fast/slow periods plus signal
    if (params.macd_enabled) {
        warmup = Math.max(warmup, params.macd_slow + params.macd_signal)
    }

    // Add extra buffer and convert to days based on candle timeframe
    warmup = Math.ceil(warmup * 1.5) // 50% extra buffer for safety
    return warmup
}

function getDateChunks(fromDate, toDate, params) {
    const chunks = []
    const warmupCandles = calculateWarmupPeriod(params)
    const candlesPerDay = Math.floor((6 * 60) / params.candle_timeframe) // ~6 hours trading day
    const warmupDays = Math.ceil(warmupCandles / candlesPerDay)

    // Adjust start date to include warmup period
    // fromDate is expected to already include any warmup adjustment.
    // Do NOT subtract warmupDays here to avoid double-adjusting the range.
    let currentFrom = momentIST(fromDate)
    const end = momentIST(toDate)

    // Define limits based on timeframe
    let daysPerChunk = 100 // Default for 5-min
    if (params.candle_timeframe === 1) daysPerChunk = 30 // 1-min has stricter limits

    while (currentFrom.isBefore(end)) {
        const chunkEnd = moment.min(
            momentIST(currentFrom).add(daysPerChunk - 1, 'days'),
            end
        )

        chunks.push({
            fromDate: currentFrom.format('YYYY-MM-DD'),
            toDate: chunkEnd.format('YYYY-MM-DD'),
            // Mark as warmup if this chunk's start is before the original (unadjusted) fromDate
            isWarmup: currentFrom.isBefore(fromDate),
        })

        currentFrom = momentIST(chunkEnd).add(1, 'days')
    }

    return chunks
}

async function fetchHistoricalData(params) {
    // For properly matching old results, we need enough warmup for the SLOWEST timeframe.
    // If we need 100 candles of 30m, that's 3000 minutes.
    const maxTfMinutes = params.candle_timeframe_number || 3;
    const htf30 = 30; // Max HTF we usually care about
    const minimumWarmupCandles = 100;

    const rsiLength = Number(params.rsi_length) || 14
    const ema1Length = Number(params.ema1_length) || 9
    const macd3Slow = Number(params.macd_3_slow) || 0
    const macd3Signal = Number(params.macd_3_signal) || 0
    const extraCandlesNeeded = Math.max(
        (minimumWarmupCandles + rsiLength) * (htf30 / maxTfMinutes),
        (Math.ceil(2.5 * ema1Length) + minimumWarmupCandles) * (htf30 / maxTfMinutes),
        params.macd_3_enabled ? (macd3Slow + macd3Signal) * 2 * (30 / maxTfMinutes) + (100 * (30 / maxTfMinutes)) : 0
    )

    const extraDays = 30 // Minimum 30 days for absolute HTF convergence

    const adjustedStartDate = momentIST(params.from_date).subtract(extraDays, 'days').format('YYYY-MM-DD')
    const chunks = getDateChunks(adjustedStartDate, params.to_date, params)
    let allCandles = []

    console.log(`[Fetch] Total chunks to fetch: ${chunks.length} (${params.candle_timeframe})`)

    for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        const progress = (((i + 1) / chunks.length) * 100).toFixed(1);
        process.stdout.write(`\r[Fetch] Progress: ${progress}% - Chunk ${i + 1}/${chunks.length} (${chunk.fromDate})...`);

        try {
            const response = await smart_api.getCandleData({
                exchange: 'NSE',
                symboltoken: params.instrument,
                interval: `${params.candle_timeframe}`,
                fromdate: momentIST(chunk.fromDate).format('YYYY-MM-DD 09:15'),
                todate: momentIST(chunk.toDate).format('YYYY-MM-DD 15:30'),
            })

            if (response?.data) {
                const candles = response.data.map((c) => ({
                    timestamp: momentIST(c[0]).format('YYYY-MM-DD HH:mm:ss'),
                    open: Number(c[1]),
                    high: Number(c[2]),
                    low: Number(c[3]),
                    close: Number(c[4]),
                    volume: Number(c[5]),
                }))
                allCandles = [...allCandles, ...candles]
            }
        } catch (err) {
            console.error(`\n[Fetch] Error in chunk ${i + 1}:`, err.message)
        }

        if (chunks.length > 1 && i < chunks.length - 1) {
            await new Promise((resolve) => setTimeout(resolve, 500)) // Reduced delay
        }
    }
    process.stdout.write('\n');
    return allCandles
}


async function calculateIndicators(candles, params) {
    if (!candles.length) return []

    // Sort candles by timestamp first to ensure correct order
    candles.sort(
        (a, b) => momentIST(a.timestamp).valueOf() - momentIST(b.timestamp).valueOf()
    )

    const closes = candles.map((c) => c.close)
    const highs = candles.map((c) => c.high)
    const lows = candles.map((c) => c.low)
    const isSupertrendMode = Boolean(params.use_supertrend_mode)
    const rsiEnabled = params.rsi_enabled !== false
    const ema1Enabled = params.ema1_enabled !== false
    const ema2Enabled = params.ema2_enabled !== false

    // Calculate RSI only when enabled.
    let rsi = [[]]
    if (rsiEnabled && params.rsi_length) {
        rsi = await new Promise((resolve, reject) => {
            tulind.indicators.rsi.indicator(
                [closes],
                [params.rsi_length],
                (err, results) => {
                    if (err) return reject(err)
                    resolve(results)
                }
            )
        })
    }

    // Calculate EMAs only when enabled.
    const [ema1Result, ema2Result] = await Promise.all([
        ema1Enabled && params.ema1_length
            ? new Promise((resolve, reject) => {
                tulind.indicators.ema.indicator(
                    [closes],
                    [params.ema1_length],
                    (err, results) => {
                        if (err) return reject(err)
                        resolve(results)
                    }
                )
            })
            : Promise.resolve([[]]),
        ema2Enabled && params.ema2_length
            ? new Promise((resolve, reject) => {
                tulind.indicators.ema.indicator(
                    [closes],
                    [params.ema2_length],
                    (err, results) => {
                        if (err) return reject(err)
                        resolve(results)
                    }
                )
            })
            : Promise.resolve([[]]),
    ])

    // Calculate MACD if enabled
    let macd = [],
        macdSignal = [],
        macdHistogram = []
    if (params.macd_enabled && !isSupertrendMode) {
        const macdRes = await new Promise((resolve, reject) => {
            tulind.indicators.macd.indicator(
                [closes],
                [params.macd_fast, params.macd_slow, params.macd_signal],
                (err, results) => {
                    if (err) return reject(err)
                    resolve(results)
                }
            )
        })
        macd = macdRes[0]
        macdSignal = macdRes[1]
        macdHistogram = macdRes[2]
    }

    let bbUpper = []
    let bbMiddle = []
    let bbLower = []


    if (params.bb_enabled) {
        const bbRes = await new Promise((resolve, reject) => {
            tulind.indicators.bbands.indicator(
                [closes],
                [params.bb_length1, params.StdDev1],
                (err, results) => {
                    if (err) return reject(err)
                    resolve(results)
                }
            )
        })


        bbLower = bbRes[0]
        bbMiddle = bbRes[1]
        bbUpper = bbRes[2]
    }

    let supertrendLine = []
    let supertrendDirection = []
    if (isSupertrendMode) {
        const atrResults = await new Promise((resolve, reject) => {
            tulind.indicators.atr.indicator(
                [highs, lows, closes],
                [params.supertrend_length],
                (err, results) => {
                    if (err) return reject(err)
                    resolve(results)
                }
            )
        })
        const atr = atrResults[0] || []
        const atrOffset = candles.length - atr.length

        let prevFinalUpper = null
        let prevFinalLower = null
        let prevTrend = null

        for (let i = 0; i < candles.length; i++) {
            const atrIdx = i - atrOffset
            if (atrIdx < 0 || atr[atrIdx] == null) {
                supertrendLine.push(null)
                supertrendDirection.push(null)
                continue
            }

            const hl2 = (highs[i] + lows[i]) / 2
            const basicUpper = hl2 + params.supertrend_factor * atr[atrIdx]
            const basicLower = hl2 - params.supertrend_factor * atr[atrIdx]
            const prevClose = i > 0 ? closes[i - 1] : closes[i]

            const finalUpper =
                prevFinalUpper == null || basicUpper < prevFinalUpper || prevClose > prevFinalUpper
                    ? basicUpper
                    : prevFinalUpper
            const finalLower =
                prevFinalLower == null || basicLower > prevFinalLower || prevClose < prevFinalLower
                    ? basicLower
                    : prevFinalLower

            let trend = prevTrend
            if (!trend) {
                trend = closes[i] >= finalLower ? 'GREEN' : 'RED'
            } else if (trend === 'GREEN' && closes[i] < finalLower) {
                trend = 'RED'
            } else if (trend === 'RED' && closes[i] > finalUpper) {
                trend = 'GREEN'
            }

            const stValue = trend === 'GREEN' ? finalLower : finalUpper
            supertrendLine.push(stValue)
            supertrendDirection.push(trend)

            prevFinalUpper = finalUpper
            prevFinalLower = finalLower
            prevTrend = trend
        }
    }
    // Now align and slice outputs to only return candles AFTER the requested start date
    const startDate = params.from_date

    // Keep a copy of original lengths
    const rsiPrevLen = rsi[0].length
    const ema1PrevLen = ema1Result[0].length
    const ema2PrevLen = ema2Result[0].length
    const macdPrevLen = macd.length

    // Filter candles to only those after the start date (use strict > like RsiEma.js)
    const postStartCandles = candles.filter((d) =>
        momentIST(d.timestamp).isAfter(momentIST(startDate))
    )

    // Slice indicator arrays to match postStartCandles length
    const startSliceRsi = Math.max(0, rsiPrevLen - postStartCandles.length)
    const startSliceEma1 = Math.max(0, ema1PrevLen - postStartCandles.length)
    const startSliceEma2 = Math.max(0, ema2PrevLen - postStartCandles.length)
    const startSliceMacd = Math.max(0, macdPrevLen - postStartCandles.length)

    const slicedRsi = rsi[0].slice(startSliceRsi)
    const slicedEma1 = ema1Result[0].slice(startSliceEma1)
    const slicedEma2 = ema2Result[0].slice(startSliceEma2)
    const slicedMacd = params.macd_enabled ? macd.slice(startSliceMacd) : []
    const slicedMacdSignal = params.macd_enabled
        ? macdSignal.slice(startSliceMacd)
        : []
    const slicedMacdHist = params.macd_enabled
        ? macdHistogram.slice(startSliceMacd)
        : []

    const bbPrevLen = bbUpper.length
    const startSliceBb = Math.max(0, bbPrevLen - postStartCandles.length)

    const slicedBbUpper = params.bb_enabled ? bbUpper.slice(startSliceBb) : []
    const slicedBbMiddle = params.bb_enabled ? bbMiddle.slice(startSliceBb) : []
    const slicedBbLower = params.bb_enabled ? bbLower.slice(startSliceBb) : []
    const slicedSupertrend = isSupertrendMode ? supertrendLine.slice(Math.max(0, supertrendLine.length - postStartCandles.length)) : []
    const slicedSupertrendDirection = isSupertrendMode ? supertrendDirection.slice(Math.max(0, supertrendDirection.length - postStartCandles.length)) : []

    // Map sliced results onto the post-start candles
    const round = (val, decimals = 2) =>
        val != null ? Number(val.toFixed(decimals)) : null;

    const candleData = postStartCandles.map((c, idx) => ({
        ...c,
        rsi: round(slicedRsi[idx], 2),
        ema1: round(slicedEma1[idx], 2),
        ema2: round(slicedEma2[idx], 2),
        macd: params.macd_enabled ? round(slicedMacd[idx], 2) : null,
        macdSignal: params.macd_enabled ? round(slicedMacdSignal[idx], 2) : null,
        macdHistogram: params.macd_enabled ? round(slicedMacdHist[idx], 2) : null,
        bbUpper: params.bb_enabled ? round(slicedBbUpper[idx], 2) : null,
        bbMiddle: params.bb_enabled ? round(slicedBbMiddle[idx], 2) : null,
        bbLower: params.bb_enabled ? round(slicedBbLower[idx], 2) : null,
        supertrend: isSupertrendMode ? round(slicedSupertrend[idx], 2) : null,
        supertrendDirection: isSupertrendMode ? slicedSupertrendDirection[idx] || null : null,
    }));

    return candleData
}

function checkEntryConditions(currentCandle, prevCandle, params, side = 'BUY', nextCandle = null) {
    // Centralized Supertrend entry logic:
    // RED -> GREEN => BUY, GREEN -> RED => SELL.
    // Entry is always taken on next candle open, so we also validate next-candle time here.
    if (params.use_supertrend_mode) {
        if (!prevCandle || !nextCandle) return false
        const entryTime = momentIST(nextCandle.timestamp).format('HH:mm')
        if (entryTime >= '15:15') return false

        const prevDirection = prevCandle.supertrendDirection
        const currentDirection = currentCandle.supertrendDirection
        if (!prevDirection || !currentDirection || prevDirection === currentDirection) {
            return false
        }

        if (side === 'BUY') {
            return prevDirection === 'RED' && currentDirection === 'GREEN'
        }
        return prevDirection === 'GREEN' && currentDirection === 'RED'
    }

    const time = momentIST(currentCandle.timestamp)

    // console.log(currentCandle)

    // Do not take entries before 9:30 AM or after 15:00
    if (time.format('HH:mm') < '09:30' || time.format('HH:mm') >= '15:10') {
        return false
    }

    if (!prevCandle) return false

    // RSI_2 check
    const rsi2 = currentCandle.rsi2
    const rsi2Buy =
        typeof params.rsi2_buy_level === 'number' ? params.rsi2_buy_level : null
    const rsi2Sell =
        typeof params.rsi2_sell_level === 'number' ? params.rsi2_sell_level : null

    // 3m BB (entry timing)
    const bbLower = currentCandle.bbLower
    const bbMiddle = currentCandle.bbMiddle
    const bbUpper = currentCandle.bbUpper

    // console.log(bbLower + "-l " + bbMiddle + " -m " + bbUpper + " -u");

    // 15m BB (trend filter)
    const bb2Lower = currentCandle.bb2Lower
    const bb2Middle = currentCandle.bb2Middle
    const bb2Upper = currentCandle.bb2Upper

    // console.log(bb2Lower + "-l " + bb2Middle + " -m " + bb2Upper + " -u");

    // Safety checks
    const hasBB3m = bbLower !== null && bbUpper !== null
    const hasBB15m = bb2Middle !== null

    if (side === 'BUY') {
        // console.log(" time -  " + currentCandle.timestamp  + "close -" + currentCandle.close + "bbMiddle - " + currentCandle.bbMiddle);
        // console.log("ADX: " + currentCandle.adx_tf )

        // console.log(
        //     "15m check:",
        //     {
        //         ts: currentCandle.timestamp,
        //         candle15mTs: currentCandle.candle15m.timestamp,
        //         close15m: currentCandle.candle15m.close,
        //         ema3_15m: currentCandle.candle15m.ema3_15m,
        //         ema4_15m: currentCandle.candle15m.ema4_15m
        //     }
        // );


        // console.log(`Evaluation Time: ${time.format('HH:mm:ss')}`);

        return (
            currentCandle.rsi > params.rsi_buy_level &&
            prevCandle.rsi < params.rsi_buy_level &&
            currentCandle.close > currentCandle.ema1 &&
            currentCandle.close > currentCandle.ema2 &&
            (rsi2Buy === null || (rsi2 !== null && rsi2 >= rsi2Buy)) &&
            // (!params.vwap_timeframe || currentCandle.close > currentCandle.vwap) &&
            (!params.macd_enabled || currentCandle.macdSignal < currentCandle.macd) &&
            (!params.macd_2_timeframe ||
                (currentCandle.macd2 !== null &&
                    currentCandle.macd2Signal !== null &&
                    currentCandle.macd2Signal < currentCandle.macd2)) &&
            (!params.macd_3_enabled ||
                (currentCandle.macd3 !== null &&
                    currentCandle.macd3Signal !== null &&
                    currentCandle.macd3Signal < currentCandle.macd3))
            &&
            (!params.ema3_15m_enabled || currentCandle.candle15m.close > currentCandle.candle15m.ema3_15m) &&
            (!params.ema4_15m_enabled || currentCandle.candle15m.close > currentCandle.candle15m.ema4_15m)

            // (!params.adx_enabled || currentCandle.adx_tf !== null && currentCandle.adx_tf > params.adx_buy_min) &&

            // (currentCandle.candle15m.close > bb2Middle &&
            //     currentCandle.low <= bbLower &&
            //     currentCandle.close > bbLower &&
            //     currentCandle.close > bbMiddle)

            // in sheet added 3 min only data
            // (!params.bb_enabled ||
            //     (bbLower !== null &&
            //         currentCandle.open > currentCandle.bbLower))

            // adds 0.50% increment in sheet

            // (!params.bb_enabled ||
            //     (bbLower !== null &&
            //         currentCandle.open > currentCandle.bb2Lower))


            // only 15m candle -> no change in result same as 3 min
            // (!params.bb_enabled ||
            //     (bbLower !== null &&
            //         currentCandle.candle15m.open > currentCandle.bb2Lower))

            // for 3m and 15m both -> no change in result
            // (params.bb_enabled &&
            //         (bbLower !== null &&
            //             currentCandle.candle15m.open > currentCandle.bb2Lower) && ( currentCandle.open > currentCandle.bbLower))


            // (!params.bb_enabled ||
            //     (bb2Middle !== null &&
            //         currentCandle.close < bb2Middle)) &&


            // (!params.bb_enabled ||
            //     (bb2Middle !== null &&
            //         currentCandle.close > bb2Middle)) &&
            // (!params.bb_enabled ||
            //     (bbLower !== null &&
            //         currentCandle.close <= bbLower * 1.002))
        )
    } else {
        // sell
        // console.log("time -  " + currentCandle.timestamp  + "close -" + currentCandle.close );
        // console.log("ADX: " + currentCandle.adx_tf )

        // console.log(
        //     "15m check:",
        //     {
        //         ts: currentCandle.timestamp,
        //         candle15mTs: currentCandle.candle15m.timestamp,
        //         close15m: currentCandle.candle15m.close,
        //         ema3_15m: currentCandle.candle15m.ema3_15m,
        //         ema4_15m: currentCandle.candle15m.ema4_15m
        //     }
        // );

        return (
            currentCandle.rsi < params.rsi_sell_level &&
            prevCandle.rsi > params.rsi_sell_level &&
            currentCandle.close < currentCandle.ema1 &&
            currentCandle.close < currentCandle.ema2 &&
            (rsi2Sell === null || (rsi2 !== null && rsi2 <= rsi2Sell)) &&
            // (!params.vwap_timeframe || currentCandle.close < currentCandle.vwap) &&
            (!params.macd_enabled || currentCandle.macdSignal > currentCandle.macd) &&
            (!params.macd_2_timeframe ||
                (currentCandle.macd2 !== null &&
                    currentCandle.macd2Signal !== null &&
                    currentCandle.macd2Signal > currentCandle.macd2)) &&
            (!params.macd_3_enabled ||
                (currentCandle.macd3 !== null &&
                    currentCandle.macd3Signal !== null &&
                    currentCandle.macd3Signal > currentCandle.macd3))
            &&
            (!params.ema3_15m_enabled || currentCandle.candle15m.close < currentCandle.candle15m.ema3_15m) &&
            (!params.ema4_15m_enabled || currentCandle.candle15m.close < currentCandle.candle15m.ema4_15m)

            // (!params.adx_enabled || currentCandle.adx_tf !== null && currentCandle.adx_tf > params.adx_sell_min) &&


            // (currentCandle.candle15m.close < bb2Middle &&
            //     currentCandle.high >= bbUpper &&
            //     currentCandle.close < bbUpper &&
            //     currentCandle.close < bbMiddle)

            // in sheet added 3 min only data
            // (!params.bb_enabled ||
            //     (bbUpper !== null &&
            //         currentCandle.close > bbMiddle))

            //  (!params.bb_enabled ||
            //         (bbUpper !== null &&
            //             currentCandle.close > bb2Middle))

            // only 15m candle -> no change in result same as 3 min
            // (!params.bb_enabled ||
            //     (bbUpper !== null &&
            //         currentCandle.candle15m.close > bb2Middle))

            // for 3m and 15m both -> no change in result
            // (params.bb_enabled &&
            // (bbUpper !== null &&
            //     currentCandle.candle15m.close > currentCandle.bb2Middle )&& (currentCandle.close > currentCandle.bbMiddle))


            // (!params.bb_enabled ||
            //     (bb2Middle !== null &&
            //         currentCandle.close > bb2Middle)) &&


            // (!params.bb_enabled ||
            //     (bb2Middle !== null &&
            //         currentCandle.close < bb2Middle)) &&
            // (!params.bb_enabled ||
            //     (bbUpper !== null &&
            //         currentCandle.close >= bbUpper * 0.998))
        )
    }
}


/**
 * Runs the backtest simulation
 */
async function runBacktest(candles, params) {
    const trades = []
    let currentPosition = null
    let equity = []
    let currentEquity = 0
    const debugEach3mClose = params.debug_each_3m_close !== false
    const n = (v) => (v == null || Number.isNaN(Number(v)) ? null : Number(Number(v).toFixed(2)))
    const isSupertrendMode = Boolean(params.use_supertrend_mode)
    const createEntryPosition = (side, currentCandle, nextCandle) => {
        // Supertrend uses next candle open; legacy strategy uses current candle close.
        const entryPrice = isSupertrendMode ? nextCandle?.open : currentCandle.close
        const entryTime = isSupertrendMode ? nextCandle?.timestamp : currentCandle.timestamp

        if (entryPrice == null || !entryTime) return null

        if (isSupertrendMode) {
            return {
                entryPrice,
                entryTime,
                side,
                indicators: {
                    supertrend: currentCandle.supertrend,
                    supertrendDirection: currentCandle.supertrendDirection,
                },
            }
        }

        return {
            entryPrice,
            entryTime,
            side,
            indicators: {
                rsi: currentCandle.rsi,
                rsi2: currentCandle.rsi2,
                ema1: currentCandle.ema1,
                ema2: currentCandle.ema2,
                macd: currentCandle.macd,
                macdSignal: currentCandle.macdSignal,
                macdHistogram: currentCandle.macdHistogram,
                macd2: currentCandle.macd2,
                macd2Signal: currentCandle.macd2Signal,
                macd2Histogram: currentCandle.macd2Histogram,
                macd3: currentCandle.macd3,
                macd3Signal: currentCandle.macd3Signal,
                macd3Histogram: currentCandle.macd3Histogram,
            },
        }
    }

    console.log(`[Backtest] Starting simulation on ${candles.length} candles...`)

    for (let i = 1; i < candles.length; i++) {
        // Progress log every 1% or every 1000 candles
        if (i % Math.max(1, Math.floor(candles.length / 100)) === 0 || i === candles.length - 1) {
            const progress = (((i + 1) / candles.length) * 100).toFixed(1);
            process.stdout.write(`\r[Backtest] Progress: ${progress}% (${i + 1}/${candles.length})`);
        }

        const currentCandle = candles[i]
        const prevCandle = candles[i - 1]
        const time = momentIST(currentCandle.timestamp)
        const nextCandle = i + 1 < candles.length ? candles[i + 1] : null

        if (debugEach3mClose) {
            const c = currentCandle;
            // console.log(`\n[DEBUG][${c.timestamp}] 3m_Close: ${c.close} | RSI: ${c.rsi} | EMA1: ${c.ema1} | EMA2: ${c.ema2}`);
            // console.log(`  MACD_3m:  ${c.macd} / ${c.macdSignal} / ${c.macdHistogram}`);

            // if (c.candle15m) {
            //     console.log(`  15m_Close: ${c.candle15m.close} | EMA3_15m: ${c.candle15m.ema3_15m} | EMA4_15m: ${c.candle15m.ema4_15m}`);
            // }

            // console.log(`  HTF_15m:  RSI2: ${c.rsi2} | MACD2: ${c.macd2} / ${c.macd2Signal} / ${c.macd2Histogram}`);
            // console.log(`  HTF_30m:  MACD3: ${c.macd3} / ${c.macd3Signal} / ${c.macd3Histogram}`);
        }

        // Handle existing position
        if (currentPosition) {
            let exitReason = null
            let exitPrice = null
            let exitTime = currentCandle.timestamp
            let supertrendBuySignal = false
            let supertrendSellSignal = false

            if (isSupertrendMode && time.format('HH:mm') >= '15:15') {
                exitReason = 'FORCED_EXIT'
                exitPrice = currentCandle.close
            } else if (isSupertrendMode) {
                // Reversal signal for currently open Supertrend position.
                supertrendBuySignal = checkEntryConditions(currentCandle, prevCandle, params, 'BUY', nextCandle)
                supertrendSellSignal = checkEntryConditions(currentCandle, prevCandle, params, 'SELL', nextCandle)
                const hasReversal = supertrendBuySignal || supertrendSellSignal

                if (hasReversal) {
                    exitReason = 'TREND_REVERSAL'
                    exitPrice = nextCandle ? nextCandle.open : currentCandle.close
                    exitTime = nextCandle ? nextCandle.timestamp : currentCandle.timestamp
                }
            } else if (time.format('HH:mm') === '15:25') {
                exitReason = 'FORCED_EXIT'
                exitPrice = currentCandle.close
            } else if (currentPosition.side === 'BUY') {
                const entry = currentPosition.entryPrice
                const targetLevel = entry + params.target_points
                const stopLevel = entry - params.stoploss_points

                const hitTarget = currentCandle.high >= targetLevel
                const hitStop = currentCandle.low <= stopLevel

                if (hitTarget && hitStop) {
                    if (currentCandle.open >= entry) {
                        exitReason = 'TARGET'
                        exitPrice = targetLevel
                    } else {
                        exitReason = 'STOPLOSS'
                        exitPrice = stopLevel
                    }
                } else if (hitTarget) {
                    exitReason = 'TARGET'
                    exitPrice = targetLevel
                } else if (hitStop) {
                    exitReason = 'STOPLOSS'
                    exitPrice = stopLevel
                }
            } else {
                const entry = currentPosition.entryPrice
                const targetLevel = entry - params.target_points
                const stopLevel = entry + params.stoploss_points

                const hitTarget = currentCandle.low <= targetLevel
                const hitStop = currentCandle.high >= stopLevel

                if (hitTarget && hitStop) {
                    if (currentCandle.open <= entry) {
                        exitReason = 'TARGET'
                        exitPrice = targetLevel
                    } else {
                        exitReason = 'STOPLOSS'
                        exitPrice = stopLevel
                    }
                } else if (hitTarget) {
                    exitReason = 'TARGET'
                    exitPrice = targetLevel
                } else if (hitStop) {
                    exitReason = 'STOPLOSS'
                    exitPrice = stopLevel
                }
            }

            if (exitReason) {
                const finalExitPrice = exitPrice != null ? exitPrice : currentCandle.close
                const pnl = currentPosition.side === 'BUY'
                    ? finalExitPrice - currentPosition.entryPrice
                    : currentPosition.entryPrice - finalExitPrice

                trades.push({
                    ...currentPosition,
                    exitPrice: finalExitPrice,
                    exitTime,
                    pnl,
                    exitReason,
                })
                currentPosition = null
                currentEquity += pnl

                // Continuous Supertrend trading:
                // on reversal, close current side and immediately open opposite side
                // at the same next-candle open.
                if (isSupertrendMode && exitReason === 'TREND_REVERSAL') {
                    if (supertrendBuySignal) {
                        currentPosition = createEntryPosition('BUY', currentCandle, nextCandle)
                    } else if (supertrendSellSignal) {
                        currentPosition = createEntryPosition('SELL', currentCandle, nextCandle)
                    }
                }
            }
        } else {
            const canBuy = checkEntryConditions(currentCandle, prevCandle, params, 'BUY', nextCandle)
            const canSell = checkEntryConditions(currentCandle, prevCandle, params, 'SELL', nextCandle)

            if (canBuy) {
                currentPosition = createEntryPosition('BUY', currentCandle, nextCandle)
            } else if (canSell) {
                currentPosition = createEntryPosition('SELL', currentCandle, nextCandle)
            }
        }
        equity.push({ timestamp: currentCandle.timestamp, equity: currentEquity })
    }
    process.stdout.write('\n');
    return generateBacktestSummary(trades, equity, params)
}

/**
 * Main controller function for the backtesting endpoint
 */
export const runBacktestStrategy = async (req, res) => {
    try {
        console.time('BacktestTotal');
        console.log(`[Backtest] Start request for ${req.body.instrument} from ${req.body.from_date} to ${req.body.to_date}`);

        const normalizedPayload = normalizePayload(req.body)
        validatePayload(normalizedPayload)

        // Fetch BASE historical data (3m candles)
        // We ensure enough warmup by adding extra days to the base fetch
        const baseTimeframe = normalizedPayload.candle_timeframe || '3m';
        const baseTfMin = timeframeToMinutes(baseTimeframe) || 3;

        const reqWithWarmup = { ...normalizedPayload, candle_timeframe_number: baseTfMin };

        // 1. Fetch all data ONCE using base timeframe
        const allCandles = await fetchHistoricalData(reqWithWarmup);
        if (!allCandles.length) throw new Error('No historical data available');

        // 2. Resample other timeframes from allCandles locally
        const tf15Enabled = !normalizedPayload.is_dynamic_timeframe_payload || normalizedPayload.timeframe_15m_enabled === true
        const tf30Enabled = !normalizedPayload.is_dynamic_timeframe_payload || normalizedPayload.timeframe_30m_enabled === true
        console.log(`[Backtest] Resampling timeframes locally...`);
        const candles15m = tf15Enabled ? resampleCandles(allCandles, 15) : [];
        const candles30m = tf30Enabled ? resampleCandles(allCandles, 30) : [];

        const rsi2TfMin = timeframeToMinutes(normalizedPayload.rsi2_timeframe);
        const macd2TfMin = timeframeToMinutes(normalizedPayload.macd_2_timeframe);
        const macd3TfMin = timeframeToMinutes(normalizedPayload.macd_3_timeframe);

        // 3. Process base candles and calculate indicators
        console.log(`[Backtest] Calculating base indicators...`);
        const processedBaseCandles = await calculateIndicators(allCandles, normalizedPayload);

        // 4. Construct Rolling HTF Snapshots (Exact match for old logic)
        if (!normalizedPayload.use_supertrend_mode) {
            console.log(`[Backtest] Constructing snapshots for 15m and 30m...`);
        }

        // Map 15m Snapshot Indicators
        const rolling15 = tf15Enabled && !normalizedPayload.use_supertrend_mode ? buildRollingHTFMap(processedBaseCandles, 15, candles15m, {
            emaPeriods: {
                ema3_15m: normalizedPayload.ema3_15m_enabled ? normalizedPayload.ema3_15m : null,
                ema4_15m: normalizedPayload.ema4_15m_enabled ? normalizedPayload.ema4_15m : null,
            },
            rsiPeriod: rsi2TfMin === 15 ? normalizedPayload.rsi2_length : null,
            macd2Config: (normalizedPayload.macd_enabled && macd2TfMin === 15) ? {
                fast: normalizedPayload.macd_2_fast, slow: normalizedPayload.macd_2_slow, signal: normalizedPayload.macd_2_signal
            } : null,
            macd3Config: (normalizedPayload.macd_3_enabled && macd3TfMin === 15) ? {
                fast: normalizedPayload.macd_3_fast, slow: normalizedPayload.macd_3_slow, signal: normalizedPayload.macd_3_signal
            } : null
        }) : new Map();

        // Map 30m Snapshot Indicators
        const rolling30 = tf30Enabled && !normalizedPayload.use_supertrend_mode ? buildRollingHTFMap(processedBaseCandles, 30, candles30m, {
            rsiPeriod: rsi2TfMin === 30 ? normalizedPayload.rsi2_length : null,
            macd2Config: (normalizedPayload.macd_enabled && macd2TfMin === 30) ? {
                fast: normalizedPayload.macd_2_fast, slow: normalizedPayload.macd_2_slow, signal: normalizedPayload.macd_2_signal
            } : null,
            macd3Config: (normalizedPayload.macd_3_enabled && macd3TfMin === 30) ? {
                fast: normalizedPayload.macd_3_fast, slow: normalizedPayload.macd_3_slow, signal: normalizedPayload.macd_3_signal
            } : null
        }) : new Map();

        const round = (val, decimals = 2) => val != null ? Number(val.toFixed(decimals)) : null;

        // 4. Final mapping of snapshots to 3m candles
        processedBaseCandles.forEach((c) => {
            const snap15 = rolling15.get(c.timestamp);
            const snap30 = rolling30.get(c.timestamp);

            if (snap15) {
                c.candle15m = {
                    ...snap15.candle,
                    ema3_15m: round(snap15.emaValues?.ema3_15m, 2),
                    ema4_15m: round(snap15.emaValues?.ema4_15m, 2),
                };
                if (rsi2TfMin === 15) c.rsi2 = round(snap15.rsi, 2);
                if (macd2TfMin === 15 && snap15.macd2) {
                    c.macd2 = round(snap15.macd2.MACD, 2);
                    c.macd2Signal = round(snap15.macd2.signal, 2);
                    c.macd2Histogram = round(snap15.macd2.histogram, 2);
                }
                if (macd3TfMin === 15 && snap15.macd3) {
                    c.macd3 = round(snap15.macd3.MACD, 2);
                    c.macd3Signal = round(snap15.macd3.signal, 2);
                    c.macd3Histogram = round(snap15.macd3.histogram, 2);
                }
            }

            if (snap30) {
                c.candle30m = { ...snap30.candle };
                if (rsi2TfMin === 30) c.rsi2 = round(snap30.rsi, 2);
                if (macd2TfMin === 30 && snap30.macd2) {
                    c.macd2 = round(snap30.macd2.MACD, 2);
                    c.macd2Signal = round(snap30.macd2.signal, 2);
                    c.macd2Histogram = round(snap30.macd2.histogram, 2);
                }
                if (macd3TfMin === 30 && snap30.macd3) {
                    c.macd3 = round(snap30.macd3.MACD, 2);
                    c.macd3Signal = round(snap30.macd3.signal, 2);
                    c.macd3Histogram = round(snap30.macd3.histogram, 2);
                }
            }
        });

        const results = await runBacktest(processedBaseCandles, normalizedPayload);
        console.timeEnd('BacktestTotal');
        res.json(results);

    } catch (error) {
        console.error('Backtest error:', error);
        res.status(400).json({ error: error.message || 'Error running backtest' });
    }
}

function splitRangeMonthWise(fromDate, toDate) {
    const ranges = []
    let cursor = momentIST(fromDate).startOf('day')
    const end = momentIST(toDate).startOf('day')

    while (cursor.isSameOrBefore(end, 'day')) {
        const monthEnd = cursor.clone().endOf('month').startOf('day')
        const segmentEnd = monthEnd.isBefore(end, 'day') ? monthEnd : end
        ranges.push({
            from_date: cursor.format('YYYY-MM-DD'),
            to_date: segmentEnd.format('YYYY-MM-DD'),
            label: cursor.format('MMMM YYYY'),
        })
        cursor = segmentEnd.clone().add(1, 'day')
    }

    return ranges
}

function calculateTradesNetPnl(trades = []) {
    return trades.reduce((acc, t) => acc + (Number(t.pnl) || 0), 0)
}

function printBacktestConsoleSummary(title, result) {
    const summary = result?.summary || {}
    const totalTrades = summary.totalTrades || 0
    const wins = summary.winningTrades || 0
    const losses = summary.losingTrades || 0
    const netPnl = calculateTradesNetPnl(result?.trades || [])
    const winRate = totalTrades > 0 ? ((wins / totalTrades) * 100).toFixed(2) + '%' : '0.00%'

    console.log('==============================')
    console.log(`BACKTEST SUMMARY - ${title}`)
    console.log(`Total Trades: ${totalTrades}`)
    console.log(`Wins: ${wins}`)
    console.log(`Losses: ${losses}`)
    console.log(`Net PnL: ${netPnl.toFixed(2)}`)
    console.log(`Win Rate: ${winRate}`)
    console.log('==============================')
}

async function runExistingBacktestAndCapture(body) {
    let statusCode = 200
    let payload
    let responded = false

    const captureRes = {
        status(code) {
            statusCode = code
            return this
        },
        json(data) {
            payload = data
            responded = true
            return data
        },
    }

    await runBacktestStrategy({ body }, captureRes)

    if (!responded) {
        throw new Error('Backtest did not return any response')
    }
    if (statusCode >= 400) {
        throw new Error(payload?.error || 'Error running backtest')
    }
    return payload
}

export const runBacktestStrategyMonthWise = async (req, res) => {
    try {
        const normalizedPayload = normalizePayload(req.body)
        validatePayload(normalizedPayload)

        const from = momentIST(normalizedPayload.from_date).startOf('day')
        const to = momentIST(normalizedPayload.to_date).startOf('day')
        const monthlyRanges = splitRangeMonthWise(normalizedPayload.from_date, normalizedPayload.to_date)
        const hasAtLeastOneFullMonth = from.clone().add(1, 'month').isSameOrBefore(to, 'day')
        const shouldRunMonthWise = monthlyRanges.length > 1 && hasAtLeastOneFullMonth

        if (!shouldRunMonthWise) {
            const singleResult = await runExistingBacktestAndCapture(req.body)
            printBacktestConsoleSummary(
                `${from.format('YYYY-MM-DD')} to ${to.format('YYYY-MM-DD')}`,
                singleResult
            )
            return res.json(singleResult)
        }

        const monthlyResults = []
        for (const range of monthlyRanges) {
            const result = await runExistingBacktestAndCapture({
                ...req.body,
                from_date: range.from_date,
                to_date: range.to_date,
            })
            monthlyResults.push({ ...range, result })
            printBacktestConsoleSummary(range.label, result)
        }

        const combinedTrades = monthlyResults
            .flatMap((item) => item.result?.trades || [])
            .slice()
            .sort(
                (a, b) =>
                    momentIST(a.exitTime || a.entryTime).valueOf() -
                    momentIST(b.exitTime || b.entryTime).valueOf()
            )

        let runningEquity = 0
        const combinedEquity = combinedTrades.map((trade) => {
            runningEquity += Number(trade.pnl) || 0
            return {
                timestamp: trade.exitTime || trade.entryTime,
                equity: runningEquity,
            }
        })

        const overall = generateBacktestSummary(combinedTrades, combinedEquity, req.body)
        const response = {
            ...overall,
            monthlySummaries: monthlyResults.map((item) => ({
                month: item.label,
                from_date: item.from_date,
                to_date: item.to_date,
                summary: item.result?.summary || {},
            })),
        }

        printBacktestConsoleSummary('OVERALL COMBINED', overall)
        return res.json(response)
    } catch (error) {
        console.error('Backtest month-wise wrapper error:', error)
        return res.status(400).json({ error: error.message || 'Error running month-wise backtest' })
    }
}



// console.log("Server Start Time (IST):", momentIST(new Date()).format('YYYY-MM-DD HH:mm:ss'));

/**
 * Generates final backtest summary and statistics
 */
function generateBacktestSummary(trades, equity, params) {
    const winningTrades = trades.filter((t) => t.pnl > 0)
    const losingTrades = trades.filter((t) => t.pnl <= 0)

    // Calculate max drawdown
    let peak = -Infinity
    let maxDD = 0
    for (const point of equity) {
        if (point.equity > peak) peak = point.equity
        const dd = peak - point.equity
        if (dd > maxDD) maxDD = dd
    }

    // Calculate average trade duration
    const durations = trades.map((t) =>
        moment(t.exitTime).diff(moment(t.entryTime), 'minutes')
    )
    const avgDuration = durations.reduce((a, b) => a + b, 0) / trades.length

    return {
        summary: {
            totalTrades: trades.length,
            winningTrades: winningTrades.length,
            losingTrades: losingTrades.length,
            winRate: (trades.length > 0 ? ((winningTrades.length / trades.length) * 100).toFixed(2) : 0) + '%',
            averagePnL: trades.length > 0 ? trades.reduce((a, b) => a + b.pnl, 0) / trades.length : 0,
            averageWinningTrade:
                winningTrades.length > 0 ? winningTrades.reduce((a, b) => a + b.pnl, 0) / winningTrades.length : 0,
            averageLosingTrade:
                losingTrades.length > 0 ? losingTrades.reduce((a, b) => a + b.pnl, 0) / losingTrades.length : 0,
            largestWin: trades.length > 0 ? Math.max(...trades.map((t) => t.pnl)) : 0,
            largestLoss: trades.length > 0 ? Math.min(...trades.map((t) => t.pnl)) : 0,
            maxDrawdown: maxDD,
            averageDurationMinutes: avgDuration || 0,
            profitFactor: losingTrades.length > 0 ? Math.abs(
                winningTrades.reduce((a, b) => a + b.pnl, 0) /
                losingTrades.reduce((a, b) => a + b.pnl, 0)
            ).toFixed(2) : (winningTrades.length > 0 ? 'Infinity' : '0.00'),
        },
        trades: trades.map((t) => ({
            ...t,
            pnlPercent: ((t.pnl / t.entryPrice) * 100).toFixed(2) + '%',
        })),
    }
}

export default runBacktestStrategy
