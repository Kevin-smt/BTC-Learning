import tulind from 'tulind'
import moment from 'moment'
import momentTimezone from 'moment-timezone'
import { SmartAPI } from 'smartapi-javascript'
import { getAngelAccessToken } from './angelTokenManager.js'
import { EMA, RSI, MACD, BollingerBands } from 'technicalindicators'

const { jwtToken, refreshToken } = await getAngelAccessToken('VIBU2601')

const smart_api = new SmartAPI({
    api_key: process.env.ANGEL_API_KEY,
    access_token: jwtToken,
    refresh_token: refreshToken,
})

// Normalize any timestamp into IST for consistent time comparisons.
const momentIST = (ts) => {
    if (typeof ts === 'string' && (ts.length <= 10 || (!ts.includes('T') && !ts.includes('Z')))) {
        return momentTimezone.tz(ts, 'Asia/Kolkata');
    }
    return momentTimezone.tz(ts, 'Asia/Kolkata');
};

// Map timeframe strings to minutes.
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
    if (key === 'ONE_DAY' || key === '1D') return 375
    return null
}

// Align a timestamp to a 09:15 anchored bucket of the selected timeframe.
function alignToMarketBucket(ts, tfMinutes) {
    const t = momentIST(ts).clone().seconds(0).milliseconds(0)
    const anchor = t.clone().startOf('day').hour(9).minute(15).second(0).millisecond(0)
    const diffMin = Math.max(0, t.diff(anchor, 'minutes'))
    const bucketIndex = Math.floor(diffMin / tfMinutes)
    return anchor.clone().add(bucketIndex * tfMinutes, 'minutes')
}

// Detect last candle of the trading day for a given timeframe.
function isLastCandleOfDay(ts, tfMinutes) {
    if (!tfMinutes || tfMinutes <= 0) return false
    const t = momentIST(ts).clone().seconds(0).milliseconds(0)
    const anchor = t.clone().startOf('day').hour(9).minute(15).second(0).millisecond(0)
    const totalMinutes = 375 // 09:15 to 15:30
    const candlesPerDay = Math.floor(totalMinutes / tfMinutes)
    if (candlesPerDay <= 0) return false
    const lastStart = anchor.clone().add((candlesPerDay - 1) * tfMinutes, 'minutes')
    return t.isSame(lastStart)
}

// Helpers to pull the latest indicator values from a series.
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

// Build rolling higher-timeframe (HTF) candles and indicators mapped to base candles.
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

// Resample base candles into a higher timeframe (anchored at 09:15).
function resampleCandles(candles, targetTimeframe) {
    if (!candles.length) return []
    const resampledCandles = []

    // Sort candles by timestamp first.
    const sorted = [...candles].sort((a, b) => momentIST(a.timestamp).valueOf() - momentIST(b.timestamp).valueOf())

    let currentBucketStart = null
    let currentGroup = []

    for (const candle of sorted) {
        // Use the same 09:15-anchored logic as alignToMarketBucket.
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

// Normalize incoming payload keys and coerce types.
function normalizePayload(rawPayload) {
    const payload = { ...rawPayload }
    // Coerce common payload types (bool/number) into normalized values.
    const toBool = (v) => {
        if (typeof v === 'boolean') return v
        if (typeof v === 'string') return v.trim().toLowerCase() === 'true'
        if (typeof v === 'number') return v !== 0
        return Boolean(v)
    }
    const toNum = (v) => {
        if (typeof v === 'number') return v
        if (typeof v === 'string' && v.trim() !== '') {
            const n = Number(v)
            return Number.isFinite(n) ? n : v
        }
        return v
    }
    const hasDynamicTfPayload = Object.prototype.hasOwnProperty.call(payload, 'timeframe_3m_enabled')
    const hasForceExit = Object.prototype.hasOwnProperty.call(payload, 'force_exit')
    const hasRestrictEntryTime = Object.prototype.hasOwnProperty.call(payload, 'restrict_entry_time')
    const forceExit = hasForceExit ? toBool(payload.force_exit) : true
    const restrictEntryTime = hasRestrictEntryTime ? toBool(payload.restrict_entry_time) : true
    // ADX (new payload) with backward-compatible aliases.
    const adxEnabled = toBool(
        Object.prototype.hasOwnProperty.call(payload, 'adx_enable')
            ? payload.adx_enable
            : payload.adx_enabled
    )
    const adxLength = toNum(
        Object.prototype.hasOwnProperty.call(payload, 'adx_length')
            ? payload.adx_length
            : payload.adx_smoothing
    )
    const adxSmoothing = toNum(payload.adx_smoothing)
    const adxLevel = toNum(payload.adx_level)
    if (!hasDynamicTfPayload) {
        const useSupertrend = toBool(payload.useSupertrend || payload.use_supertrend_mode)
        return {
            ...payload,
            is_dynamic_timeframe_payload: false,
            force_exit: forceExit,
            restrict_entry_time: restrictEntryTime,
            adx_enabled: adxEnabled,
            adx_length: adxLength,
            adx_smoothing: adxSmoothing,
            adx_level: adxLevel,
            use_supertrend_mode: useSupertrend,
            // Keep base-indicator flags independent from Supertrend.
            // When Supertrend is enabled, it becomes an additional condition, not a replacement.
            rsi_enabled: payload.rsi_enabled,
            ema1_enabled: payload.ema1_enabled,
            ema2_enabled: payload.ema2_enabled,
            macd_enabled: payload.macd_enabled,
            supertrend_length: toNum(payload.supertrend_length),
            supertrend_factor: toNum(payload.supertrend_factor),
        }
    }

    const tf1Enabled = toBool(payload.timeframe_3m_enabled)
    const tf1 = payload.tf_timeframe_one || 'THREE_MINUTE'
    const tf1Minutes = timeframeToMinutes(tf1)
    const useSupertrend = tf1Enabled && toBool(payload.tf_one_useSupertrend)
    const useThreeSupertrend =
        tf1Enabled &&
        !useSupertrend &&
        toBool(payload.three_super_trend)
    const tf2Enabled = toBool(payload.timeframe_15m_enabled)
    const tf3Enabled = toBool(payload.timeframe_30m_enabled)
    const tf2 = payload.tf_timeframe_two || 'FIFTEEN_MINUTE'
    const tf3 = payload.tf_timeframe_three || 'THIRTY_MINUTE'
    const tf2Minutes = timeframeToMinutes(tf2)
    const tf3Minutes = timeframeToMinutes(tf3)

    const rsi2FromTf2 = tf2Enabled && toBool(payload.tf_two_isrsi)
    const rsi2FromTf3 = tf3Enabled && toBool(payload.tf_three_isrsi)
    const useRsi2 = rsi2FromTf2 || rsi2FromTf3

    const useMacd2 = tf2Enabled && toBool(payload.tf_two_ismacd)
    const useMacd3 = tf3Enabled && toBool(payload.tf_three_ismacd)

    const useEma3_15m =
        tf2Enabled &&
        tf2Minutes === 15 &&
        toBool(payload.tf_two_isema1)
    const useEma4_15m =
        tf2Enabled &&
        tf2Minutes === 15 &&
        toBool(payload.tf_two_isema2)

    return {
        ...payload,
        is_dynamic_timeframe_payload: true,
        force_exit: forceExit,
        restrict_entry_time: restrictEntryTime,
        adx_enabled: adxEnabled,
        adx_length: adxLength,
        adx_smoothing: adxSmoothing,
        adx_level: adxLevel,
        candle_timeframe: tf1,
        candle_timeframe_number: tf1Minutes || 3,

        rsi_length: toNum(payload.tf_one_rsi_length),
        rsi_buy_level: toNum(payload.tf_one_rsi_buy_level),
        rsi_sell_level: toNum(payload.tf_one_rsi_sell_level),
        ema1_length: toNum(payload.tf_one_ema1),
        ema2_length: toNum(payload.tf_one_ema2),
        macd_fast: toNum(payload.tf_one_macd_fast),
        macd_slow: toNum(payload.tf_one_macd_slow),
        macd_signal: toNum(payload.tf_one_macd_signal),

        // indicator gates
        rsi_enabled: tf1Enabled && toBool(payload.tf_one_isrsi),
        ema1_enabled: tf1Enabled && toBool(payload.tf_one_isema1),
        ema2_enabled: tf1Enabled && toBool(payload.tf_one_isema2),
        macd_enabled: tf1Enabled && toBool(payload.tf_one_ismacd),

        // Supertrend mode + params for the active base timeframe (tf_one).
        use_supertrend_mode: useSupertrend,
        supertrend_length: toNum(payload.tf_one_supertrend_length),
        supertrend_factor: toNum(payload.tf_one_supertrend_factor),
        use_three_supertrend_mode: useThreeSupertrend,
        supertrend_one_length: toNum(payload.supertrend_one_length),
        supertrend_one_factor: toNum(payload.supertrend_one_factor),
        supertrend_two_length: toNum(payload.supertrend_two_length),
        supertrend_two_factor: toNum(payload.supertrend_two_factor),
        supertrend_three_length: toNum(payload.supertrend_three_length),
        supertrend_three_factor: toNum(payload.supertrend_three_factor),

        // keep legacy engine happy while supertrend mode ignores these
        target_points:
            typeof toNum(payload.target_points) === 'number' ? toNum(payload.target_points) : 1,
        stoploss_points:
            typeof toNum(payload.stoploss_points) === 'number' ? toNum(payload.stoploss_points) : 1,

        // map dynamic HTF payload to legacy engine keys used by /backtest
        // NOTE: these remain active even when Supertrend is enabled, so HTF confirmations
        // can still participate in the final AND condition if user enabled them.
        rsi2_timeframe: rsi2FromTf2 ? tf2 : (rsi2FromTf3 ? tf3 : null),
        rsi2_length: useRsi2
            ? toNum(rsi2FromTf2 ? payload.tf_two_rsi_length : payload.tf_three_rsi_length)
            : null,
        rsi2_buy_level: useRsi2
            ? toNum(rsi2FromTf2 ? payload.tf_two_rsi_buy_level : payload.tf_three_rsi_buy_level)
            : null,
        rsi2_sell_level: useRsi2
            ? toNum(rsi2FromTf2 ? payload.tf_two_rsi_sell_level : payload.tf_three_rsi_sell_level)
            : null,

        macd_2_timeframe: useMacd2 ? tf2 : null,
        macd_2_fast: toNum(useMacd2 ? payload.tf_two_macd_fast : null),
        macd_2_slow: toNum(useMacd2 ? payload.tf_two_macd_slow : null),
        macd_2_signal: toNum(useMacd2 ? payload.tf_two_macd_signal : null),

        macd_3_enabled: useMacd3,
        macd_3_timeframe: useMacd3 ? tf3 : null,
        macd_3_fast: toNum(useMacd3 ? payload.tf_three_macd_fast : null),
        macd_3_slow: toNum(useMacd3 ? payload.tf_three_macd_slow : null),
        macd_3_signal: toNum(useMacd3 ? payload.tf_three_macd_signal : null),

        ema3_15m_enabled: useEma3_15m,
        ema3_15m: toNum(useEma3_15m ? payload.tf_two_ema1 : null),
        ema4_15m_enabled: useEma4_15m,
        ema4_15m: toNum(useEma4_15m ? payload.tf_two_ema2 : null),
    }
}

// Validate required fields and config ranges.
function validatePayload(payload) {
    const supertrendMode = Boolean(payload.use_supertrend_mode)
    const threeSupertrendMode = Boolean(payload.use_three_supertrend_mode)
    if (payload.is_dynamic_timeframe_payload && payload.timeframe_3m_enabled === false) {
        throw new Error('timeframe_3m_enabled must be true for this strategy')
    }
    const required = [
        'instrument',
        'candle_timeframe',
        'from_date',
        'to_date',
    ]
    if (!supertrendMode && !threeSupertrendMode) {
        required.push(
            'rsi_length',
            'rsi_buy_level',
            'rsi_sell_level',
            'ema1_length',
            'ema2_length',
            'target_points',
            'stoploss_points'
        )
    } else if (supertrendMode) {
        required.push('supertrend_length', 'supertrend_factor')
    } else if (threeSupertrendMode) {
        required.push(
            'supertrend_one_length',
            'supertrend_one_factor',
            'supertrend_two_length',
            'supertrend_two_factor',
            'supertrend_three_length',
            'supertrend_three_factor'
        )
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
    if (!supertrendMode && !threeSupertrendMode) {
        numericFields.push(
            'rsi_length',
            'rsi_buy_level',
            'rsi_sell_level',
            'ema1_length',
            'ema2_length',
            'target_points',
            'stoploss_points'
        )
    } else if (supertrendMode) {
        numericFields.push('supertrend_length', 'supertrend_factor')
    } else if (threeSupertrendMode) {
        numericFields.push(
            'supertrend_one_length',
            'supertrend_one_factor',
            'supertrend_two_length',
            'supertrend_two_factor',
            'supertrend_three_length',
            'supertrend_three_factor'
        )
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

    if (payload.adx_enabled) {
        if (typeof payload.adx_length !== 'number' || payload.adx_length <= 0) {
            throw new Error('adx_length must be a positive number when ADX is enabled')
        }
        if (typeof payload.adx_level !== 'number' || payload.adx_level <= 0) {
            throw new Error('adx_level must be a positive number when ADX is enabled')
        }
        // Backward compatibility: validate adx_timeframe only if provided.
        if (payload.adx_timeframe) {
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
    }
    return true
}

// Determine the minimum warmup period needed for stable indicators.
function calculateWarmupPeriod(params) {
    // RSI needs rsi_length periods
    let warmup = Number(params.rsi_length) || 14

    // EMA needs roughly 2.5 times the period for proper convergence
    warmup = Math.max(warmup, Math.ceil(2.5 * (Number(params.ema1_length) || 9)))
    warmup = Math.max(warmup, Math.ceil(2.5 * (Number(params.ema2_length) || 21)))

    if (params.use_supertrend_mode) {
        warmup = Math.max(warmup, Number(params.supertrend_length) || 10)
    }
    if (params.use_three_supertrend_mode) {
        warmup = Math.max(
            warmup,
            Number(params.supertrend_one_length) || 10,
            Number(params.supertrend_two_length) || 10,
            Number(params.supertrend_three_length) || 10
        )
    }

    if (params.ema3_15m_enabled) {
        warmup = Math.max(warmup, Math.ceil(2.5 * params.ema3_15m))
    }

    if (params.ema4_15m_enabled) {
        warmup = Math.max(warmup, Math.ceil(2.5 * params.ema4_15m))
    }


    // MACD needs the longer of fast/slow periods plus signal.
    if (params.macd_enabled) {
        warmup = Math.max(warmup, params.macd_slow + params.macd_signal)
    }

    // Add extra buffer and convert to days based on candle timeframe
    warmup = Math.ceil(warmup * 1.5) // 50% extra buffer for safety
    return warmup
}

// Split large date ranges into API-friendly chunks, preserving warmup.
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

    // Define limits based on timeframe.
    // Tune these values if you want larger/smaller API chunks per request.
    let daysPerChunk = 60 // Default baseline (safer for rate limits)
    if (params.candle_timeframe === 1) daysPerChunk = 10 // 1-min has stricter limits
    if (params.candle_timeframe === 3) daysPerChunk = 20
    if (params.candle_timeframe === 5) daysPerChunk = 30
    if (params.candle_timeframe === 15) daysPerChunk = 60
    if (params.candle_timeframe === 30) daysPerChunk = 90

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

    // Base delay before each API call (increase to reduce rate-limit errors).
    const baseDelayMs = 5000
    // Max retry attempts for rate-limit errors.
    const maxRetries = 5

    const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
    // Jitter to avoid thundering herd (adjust range if needed).
    const jitter = (ms) => Math.floor(ms * (0.1 + Math.random() * 0.2))

    for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i];
        const progress = (((i + 1) / chunks.length) * 100).toFixed(1);
        process.stdout.write(`\r[Fetch] Progress: ${progress}% - Chunk ${i + 1}/${chunks.length} (${chunk.fromDate})...`);

        const fromdate = momentIST(chunk.fromDate).format('YYYY-MM-DD 09:15')
        const todate = momentIST(chunk.toDate).format('YYYY-MM-DD 15:30')

        for (let attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                // Throttle: wait before every API call.
                await sleep(baseDelayMs + jitter(baseDelayMs))

                const response = await smart_api.getCandleData({
                    exchange: 'NSE',
                    symboltoken: params.instrument,
                    interval: `${params.candle_timeframe}`,
                    fromdate,
                    todate,
                })

                if (!response || !Array.isArray(response.data)) {
                    const errorCode = response?.errorcode || response?.message
                    if (errorCode === 'AB1004' || errorCode === 'TooManyRequests') {
                        // Backoff with cap (adjust cap if you want longer waits).
                        const backoffMs = Math.min(60000, baseDelayMs * (2 ** attempt))
                        console.warn(
                            `\n[Fetch] Rate limited (AB1004). Backing off ${backoffMs}ms (attempt ${attempt + 1}/${maxRetries + 1})`
                        )
                        await sleep(backoffMs + jitter(backoffMs))
                        continue
                    }
                    console.error(
                        `\n[Fetch] Empty/invalid response in chunk ${i + 1} ` +
                        `(from=${fromdate}, to=${todate})`,
                        response
                    )
                    break
                }

                if (response.data.length === 0) {
                    console.warn(
                        `\n[Fetch] Zero candles in chunk ${i + 1} ` +
                        `(from=${fromdate}, to=${todate})`
                    )
                    break
                }

                const candles = response.data.map((c) => ({
                    timestamp: momentIST(c[0]).format('YYYY-MM-DD HH:mm:ss'),
                    open: Number(c[1]),
                    high: Number(c[2]),
                    low: Number(c[3]),
                    close: Number(c[4]),
                    volume: Number(c[5]),
                }))
                allCandles = [...allCandles, ...candles]
                break
            } catch (err) {
                const errorCode = err?.errorcode || err?.message
                if (errorCode === 'AB1004' || errorCode === 'TooManyRequests') {
                    // Backoff with cap (adjust cap if you want longer waits).
                    const backoffMs = Math.min(60000, baseDelayMs * (2 ** attempt))
                    console.warn(
                        `\n[Fetch] Rate limited (AB1004). Backing off ${backoffMs}ms (attempt ${attempt + 1}/${maxRetries + 1})`
                    )
                    await sleep(backoffMs + jitter(backoffMs))
                    continue
                }
                console.error(`\n[Fetch] Error in chunk ${i + 1}:`, err.message)
                break
            }
        }

        if (chunks.length > 1 && i < chunks.length - 1) {
            // Throttle API calls between chunk requests.
            await sleep(baseDelayMs + jitter(baseDelayMs))
        }
    }
    process.stdout.write('\n');
    return allCandles
}


// Calculate base timeframe indicators and slice to requested date range.
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
    const isThreeSupertrendMode = Boolean(params.use_three_supertrend_mode)
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

    // Calculate ADX only when enabled (uses base timeframe highs/lows/closes).
    // Note: tulind ADX uses a single period input. We prefer adx_length and
    // fall back to adx_smoothing if provided.
    let adx = [[]]
    if (params.adx_enabled && (params.adx_length || params.adx_smoothing)) {
        const adxPeriod = Number(params.adx_length || params.adx_smoothing)
        adx = await new Promise((resolve, reject) => {
            tulind.indicators.adx.indicator(
                [highs, lows, closes],
                [adxPeriod],
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
    if (params.macd_enabled) {
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

    // Supertrend calculation helper for reuse (single or multiple setups)
    const computeSupertrend = async (length, factor) => {
        const atrResults = await new Promise((resolve, reject) => {
            tulind.indicators.atr.indicator(
                [highs, lows, closes],
                [length],
                (err, results) => {
                    if (err) return reject(err)
                    resolve(results)
                }
            )
        })
        const atr = atrResults[0] || []
        const atrOffset = candles.length - atr.length

        const line = []
        const direction = []
        let prevFinalUpper = null
        let prevFinalLower = null
        let prevTrend = null

        for (let i = 0; i < candles.length; i++) {
            const atrIdx = i - atrOffset
            if (atrIdx < 0 || atr[atrIdx] == null) {
                line.push(null)
                direction.push(null)
                continue
            }

            const hl2 = (highs[i] + lows[i]) / 2
            const basicUpper = hl2 + factor * atr[atrIdx]
            const basicLower = hl2 - factor * atr[atrIdx]
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
            line.push(stValue)
            direction.push(trend)

            prevFinalUpper = finalUpper
            prevFinalLower = finalLower
            prevTrend = trend
        }

        return { line, direction }
    }

    let supertrendLine = []
    let supertrendDirection = []
    let supertrend1 = []
    let supertrend1Direction = []
    let supertrend2 = []
    let supertrend2Direction = []
    let supertrend3 = []
    let supertrend3Direction = []

    if (isSupertrendMode) {
        const { line, direction } = await computeSupertrend(
            params.supertrend_length,
            params.supertrend_factor
        )
        supertrendLine = line
        supertrendDirection = direction
    }

    if (isThreeSupertrendMode) {
        const st1 = await computeSupertrend(
            params.supertrend_one_length,
            params.supertrend_one_factor
        )
        const st2 = await computeSupertrend(
            params.supertrend_two_length,
            params.supertrend_two_factor
        )
        const st3 = await computeSupertrend(
            params.supertrend_three_length,
            params.supertrend_three_factor
        )
        supertrend1 = st1.line
        supertrend1Direction = st1.direction
        supertrend2 = st2.line
        supertrend2Direction = st2.direction
        supertrend3 = st3.line
        supertrend3Direction = st3.direction
    }
    // Now align and slice outputs to only return candles AFTER the requested start date
    const startDate = params.from_date

    // Keep a copy of original lengths
    const rsiPrevLen = rsi[0].length
    const adxPrevLen = adx[0].length
    const ema1PrevLen = ema1Result[0].length
    const ema2PrevLen = ema2Result[0].length
    const macdPrevLen = macd.length

    // Filter candles to only those after the start date (use strict > like RsiEma.js)
    const postStartCandles = candles.filter((d) =>
        momentIST(d.timestamp).isAfter(momentIST(startDate))
    )

    // Slice indicator arrays to match postStartCandles length
    const startSliceRsi = Math.max(0, rsiPrevLen - postStartCandles.length)
    const startSliceAdx = Math.max(0, adxPrevLen - postStartCandles.length)
    const startSliceEma1 = Math.max(0, ema1PrevLen - postStartCandles.length)
    const startSliceEma2 = Math.max(0, ema2PrevLen - postStartCandles.length)
    const startSliceMacd = Math.max(0, macdPrevLen - postStartCandles.length)

    const slicedRsi = rsi[0].slice(startSliceRsi)
    const slicedAdx = adx[0].slice(startSliceAdx)
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
    const slicedSupertrend = isSupertrendMode
        ? supertrendLine.slice(Math.max(0, supertrendLine.length - postStartCandles.length))
        : []
    const slicedSupertrendDirection = isSupertrendMode
        ? supertrendDirection.slice(Math.max(0, supertrendDirection.length - postStartCandles.length))
        : []
    const slicedSupertrend1 = isThreeSupertrendMode
        ? supertrend1.slice(Math.max(0, supertrend1.length - postStartCandles.length))
        : []
    const slicedSupertrend1Direction = isThreeSupertrendMode
        ? supertrend1Direction.slice(Math.max(0, supertrend1Direction.length - postStartCandles.length))
        : []
    const slicedSupertrend2 = isThreeSupertrendMode
        ? supertrend2.slice(Math.max(0, supertrend2.length - postStartCandles.length))
        : []
    const slicedSupertrend2Direction = isThreeSupertrendMode
        ? supertrend2Direction.slice(Math.max(0, supertrend2Direction.length - postStartCandles.length))
        : []
    const slicedSupertrend3 = isThreeSupertrendMode
        ? supertrend3.slice(Math.max(0, supertrend3.length - postStartCandles.length))
        : []
    const slicedSupertrend3Direction = isThreeSupertrendMode
        ? supertrend3Direction.slice(Math.max(0, supertrend3Direction.length - postStartCandles.length))
        : []

    // Map sliced results onto the post-start candles
    const round = (val, decimals = 2) =>
        val != null ? Number(val.toFixed(decimals)) : null;

    const candleData = postStartCandles.map((c, idx) => ({
        ...c,
        rsi: round(slicedRsi[idx], 2),
        adx: params.adx_enabled ? round(slicedAdx[idx], 2) : null,
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
        // Three-supertrend outputs (optional, for debugging/verification)
        supertrend1: isThreeSupertrendMode ? round(slicedSupertrend1[idx], 2) : null,
        supertrend1Direction: isThreeSupertrendMode ? slicedSupertrend1Direction[idx] || null : null,
        supertrend2: isThreeSupertrendMode ? round(slicedSupertrend2[idx], 2) : null,
        supertrend2Direction: isThreeSupertrendMode ? slicedSupertrend2Direction[idx] || null : null,
        supertrend3: isThreeSupertrendMode ? round(slicedSupertrend3[idx], 2) : null,
        supertrend3Direction: isThreeSupertrendMode ? slicedSupertrend3Direction[idx] || null : null,
    }));

    return candleData
}

// Evaluate entry rules for a side using the current closed candle.
function checkEntryConditions(currentCandle, prevCandle, params, side = 'BUY', nextCandle = null) {
    // Signal is evaluated on current CLOSED candle and executed at next candle open.
    if (!prevCandle || !nextCandle) return false
    // Optional time gating (e.g., only after 09:30 and before last tradable entry).
    if (params.restrict_entry_time !== false) {
        const evalTime = momentIST(currentCandle.timestamp).format('HH:mm')
        const entryTime = momentIST(nextCandle.timestamp).format('HH:mm')
        if (evalTime < '09:30' || evalTime >= '15:10') return false
        if (entryTime >= '15:15') return false
    }

    // Supertrend direction change for the active timeframe (single supertrend mode).
    const prevDirection = prevCandle.supertrendDirection
    const currentDirection = currentCandle.supertrendDirection
    const singleSupertrendSideSignal =
        prevDirection &&
            currentDirection &&
            prevDirection !== currentDirection
            ? (prevDirection === 'RED' && currentDirection === 'GREEN' ? 'BUY' : 'SELL')
            : null

    // Three supertrend alignment flip:
    // BUY when all 3 become GREEN; SELL when all 3 become RED.
    const prevDirs = [
        prevCandle.supertrend1Direction,
        prevCandle.supertrend2Direction,
        prevCandle.supertrend3Direction,
    ]
    const currentDirs = [
        currentCandle.supertrend1Direction,
        currentCandle.supertrend2Direction,
        currentCandle.supertrend3Direction,
    ]
    const prevAllGreen = prevDirs.every((d) => d === 'GREEN')
    const prevAllRed = prevDirs.every((d) => d === 'RED')
    const currentAllGreen = currentDirs.every((d) => d === 'GREEN')
    const currentAllRed = currentDirs.every((d) => d === 'RED')
    const threeSupertrendSideSignal =
        !prevAllGreen && currentAllGreen
            ? 'BUY'
            : !prevAllRed && currentAllRed
                ? 'SELL'
                : null

    const rsi2 = currentCandle.rsi2
    const rsi2Buy = typeof params.rsi2_buy_level === 'number' ? params.rsi2_buy_level : null
    const rsi2Sell = typeof params.rsi2_sell_level === 'number' ? params.rsi2_sell_level : null
    const conditions = []

    // Base timeframe conditions (include only when their flags are enabled).
    if (params.rsi_enabled !== false) {
        conditions.push(
            side === 'BUY'
                ? currentCandle.rsi > params.rsi_buy_level && prevCandle.rsi < params.rsi_buy_level
                : currentCandle.rsi < params.rsi_sell_level && prevCandle.rsi > params.rsi_sell_level
        )
    }
    if (params.ema1_enabled !== false) {
        conditions.push(side === 'BUY' ? currentCandle.close > currentCandle.ema1 : currentCandle.close < currentCandle.ema1)
    }
    if (params.ema2_enabled !== false) {
        conditions.push(side === 'BUY' ? currentCandle.close > currentCandle.ema2 : currentCandle.close < currentCandle.ema2)
    }
    if (params.macd_enabled) {
        // MACD confirmation (blue MACD line vs red signal line).
        conditions.push(side === 'BUY' ? currentCandle.macd > currentCandle.macdSignal : currentCandle.macd < currentCandle.macdSignal)
    }
    if (params.use_supertrend_mode) {
        // Single Supertrend is directional trigger when enabled.
        conditions.push(singleSupertrendSideSignal === side)
    }
    if (params.use_three_supertrend_mode) {
        // Three Supertrend setup triggers only on full alignment flips.
        conditions.push(threeSupertrendSideSignal === side)
    }
    if (params.adx_enabled) {
        // ADX acts as an additional entry filter.
        conditions.push(currentCandle.adx != null && currentCandle.adx > params.adx_level)
    }

    // Higher timeframe confirmations.
    if (rsi2Buy !== null || rsi2Sell !== null) {
        conditions.push(
            side === 'BUY'
                ? (rsi2Buy === null || (rsi2 !== null && rsi2 >= rsi2Buy))
                : (rsi2Sell === null || (rsi2 !== null && rsi2 <= rsi2Sell))
        )
    }
    if (params.macd_2_timeframe) {
        conditions.push(
            side === 'BUY'
                ? (currentCandle.macd2 !== null && currentCandle.macd2Signal !== null && currentCandle.macd2 > currentCandle.macd2Signal)
                : (currentCandle.macd2 !== null && currentCandle.macd2Signal !== null && currentCandle.macd2 < currentCandle.macd2Signal)
        )
    }
    if (params.macd_3_enabled) {
        conditions.push(
            side === 'BUY'
                ? (currentCandle.macd3 !== null && currentCandle.macd3Signal !== null && currentCandle.macd3 > currentCandle.macd3Signal)
                : (currentCandle.macd3 !== null && currentCandle.macd3Signal !== null && currentCandle.macd3 < currentCandle.macd3Signal)
        )
    }
    if (params.ema3_15m_enabled) {
        conditions.push(side === 'BUY' ? currentCandle.candle15m.close > currentCandle.candle15m.ema3_15m : currentCandle.candle15m.close < currentCandle.candle15m.ema3_15m)
    }
    if (params.ema4_15m_enabled) {
        conditions.push(side === 'BUY' ? currentCandle.candle15m.close > currentCandle.candle15m.ema4_15m : currentCandle.candle15m.close < currentCandle.candle15m.ema4_15m)
    }

    if (!conditions.length) return false
    return conditions.every(Boolean)
}


/**
 * Runs the backtest simulation.
 */
async function runBacktest(candles, params) {
    const trades = []
    let currentPosition = null
    let equity = []
    let currentEquity = 0
    const isSupertrendMode = Boolean(params.use_supertrend_mode)
    const isThreeSupertrendMode = Boolean(params.use_three_supertrend_mode)
    const useFixedTarget = params.fixtarget === true || params.fixtarget === 'true'
    const forceExit = params.force_exit !== false
    const tfMinutes =
        timeframeToMinutes(params.candle_timeframe) ||
        params.candle_timeframe_number ||
        3

    // Keep trade candle snapshots for easier verification in Postman.
    const snapshotCandle = (candle) =>
        candle
            ? {
                timestamp: candle.timestamp,
                open: candle.open,
                high: candle.high,
                low: candle.low,
                close: candle.close,
            }
            : null

    // Supertrend reversal side for the current closed candle (single supertrend).
    const getSupertrendFlipSide = (prevCandle, currentCandle) => {
        if (!isSupertrendMode) return null
        const prevDirection = prevCandle?.supertrendDirection
        const currentDirection = currentCandle?.supertrendDirection
        if (!prevDirection || !currentDirection || prevDirection === currentDirection) return null
        return prevDirection === 'RED' && currentDirection === 'GREEN' ? 'BUY' : 'SELL'
    }

    // Three-supertrend reversal side (full alignment flip).
    const getThreeSupertrendFlipSide = (prevCandle, currentCandle) => {
        if (!isThreeSupertrendMode) return null
        const prevDirs = [
            prevCandle?.supertrend1Direction,
            prevCandle?.supertrend2Direction,
            prevCandle?.supertrend3Direction,
        ]
        const currentDirs = [
            currentCandle?.supertrend1Direction,
            currentCandle?.supertrend2Direction,
            currentCandle?.supertrend3Direction,
        ]
        if (prevDirs.some((d) => !d) || currentDirs.some((d) => !d)) return null
        const prevAllGreen = prevDirs.every((d) => d === 'GREEN')
        const prevAllRed = prevDirs.every((d) => d === 'RED')
        const currentAllGreen = currentDirs.every((d) => d === 'GREEN')
        const currentAllRed = currentDirs.every((d) => d === 'RED')
        if (!prevAllGreen && currentAllGreen) return 'BUY'
        if (!prevAllRed && currentAllRed) return 'SELL'
        return null
    }

    // Exit rule for three-supertrend: exit when at least 2 of 3 flip against position.
    const shouldExitThreeSupertrend = (positionSide, currentCandle) => {
        const dirs = [
            currentCandle?.supertrend1Direction,
            currentCandle?.supertrend2Direction,
            currentCandle?.supertrend3Direction,
        ]
        if (dirs.some((d) => !d)) return false
        const greens = dirs.filter((d) => d === 'GREEN').length
        const reds = dirs.filter((d) => d === 'RED').length
        if (positionSide === 'BUY') return reds >= 2
        if (positionSide === 'SELL') return greens >= 2
        return false
    }

    // Build entry position consistently in one place.
    const createEntryPosition = (side, currentCandle, nextCandle, entryReason) => {
        const entryPrice = nextCandle?.open
        const entryTime = nextCandle?.timestamp

        if (entryPrice == null || !entryTime) return null

        // For "entry time" indicators, prefer the actual entry candle (nextCandle).
        const entryIndicatorCandle = nextCandle || currentCandle

        return {
            entryPrice,
            entryTime,
            side,
            entryReason,
            adx_value: currentCandle.adx,
            entry_supertrend1: isThreeSupertrendMode ? entryIndicatorCandle?.supertrend1 ?? null : null,
            entry_supertrend2: isThreeSupertrendMode ? entryIndicatorCandle?.supertrend2 ?? null : null,
            entry_supertrend3: isThreeSupertrendMode ? entryIndicatorCandle?.supertrend3 ?? null : null,
            entryCandle: snapshotCandle(nextCandle),
            indicators: {
                rsi: currentCandle.rsi,
                rsi2: currentCandle.rsi2,
                adx: currentCandle.adx,
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
                supertrend: currentCandle.supertrend,
                supertrendDirection: currentCandle.supertrendDirection,
            },
        }
    }

    // Centralized exit evaluator (forced exits, trend reversals, and target/stoploss).
    const evaluateExit = (position, currentCandle, prevCandle, nextCandle) => {
        let exitReason = null
        let exitPrice = null
        let exitTime = currentCandle.timestamp
        let exitCandle = snapshotCandle(currentCandle)
        let supertrendFlipSide = null

        if (forceExit && isLastCandleOfDay(currentCandle.timestamp, tfMinutes)) {
            exitReason = 'FORCED_EXIT'
            exitPrice = currentCandle.close
            return { exitReason, exitPrice, exitTime, exitCandle, supertrendFlipSide }
        }

        if (isSupertrendMode) {
            supertrendFlipSide = getSupertrendFlipSide(prevCandle, currentCandle)
            if (supertrendFlipSide) {
                exitReason = 'TREND_REVERSAL'
                exitPrice = nextCandle ? nextCandle.open : currentCandle.close
                exitTime = nextCandle ? nextCandle.timestamp : currentCandle.timestamp
                exitCandle = nextCandle ? snapshotCandle(nextCandle) : snapshotCandle(currentCandle)
            }
            return { exitReason, exitPrice, exitTime, exitCandle, supertrendFlipSide }
        }
        if (isThreeSupertrendMode) {
            supertrendFlipSide = getThreeSupertrendFlipSide(prevCandle, currentCandle)
            if (shouldExitThreeSupertrend(position.side, currentCandle)) {
                exitReason = 'TREND_REVERSAL'
                exitPrice = nextCandle ? nextCandle.open : currentCandle.close
                exitTime = nextCandle ? nextCandle.timestamp : currentCandle.timestamp
                exitCandle = nextCandle ? snapshotCandle(nextCandle) : snapshotCandle(currentCandle)
            }
            return { exitReason, exitPrice, exitTime, exitCandle, supertrendFlipSide }
        }

        // Target/Stoploss are applied only when fixtarget is true.
        if (!useFixedTarget) {
            return { exitReason, exitPrice, exitTime, exitCandle, supertrendFlipSide }
        }

        if (position.side === 'BUY') {
            const entry = position.entryPrice
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
            const entry = position.entryPrice
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

        return { exitReason, exitPrice, exitTime, exitCandle, supertrendFlipSide }
    }

    console.log(`[Backtest] Starting simulation on ${candles.length} candles...`)

    for (let i = 1; i < candles.length; i++) {
        // Progress log every 1% or every 1000 candles.
        if (i % Math.max(1, Math.floor(candles.length / 100)) === 0 || i === candles.length - 1) {
            const progress = (((i + 1) / candles.length) * 100).toFixed(1);
            process.stdout.write(`\r[Backtest] Progress: ${progress}% (${i + 1}/${candles.length})`);
        }

        const currentCandle = candles[i]
        const prevCandle = candles[i - 1]
        const nextCandle = i + 1 < candles.length ? candles[i + 1] : null

        if (currentPosition) {
            const { exitReason, exitPrice, exitTime, exitCandle, supertrendFlipSide } =
                evaluateExit(currentPosition, currentCandle, prevCandle, nextCandle)

            if (exitReason) {
                const finalExitPrice = exitPrice != null ? exitPrice : currentCandle.close
                // For "exit time" indicators, prefer the actual exit candle.
                const exitIndicatorCandle =
                    nextCandle && exitTime === nextCandle.timestamp ? nextCandle : currentCandle
                const durationMinutes = momentIST(exitTime).diff(
                    momentIST(currentPosition.entryTime),
                    'minutes',
                    true
                )
                const activeTimeHours = Number((durationMinutes / 60).toFixed(2))
                const pnl = currentPosition.side === 'BUY'
                    ? finalExitPrice - currentPosition.entryPrice
                    : currentPosition.entryPrice - finalExitPrice

                trades.push({
                    ...currentPosition,
                    exitPrice: finalExitPrice,
                    exitTime,
                    exitCandle,
                    pnl,
                    exitReason,
                    active_time: activeTimeHours,
                    exit_supertrend1: isThreeSupertrendMode ? exitIndicatorCandle?.supertrend1 ?? null : null,
                    exit_supertrend2: isThreeSupertrendMode ? exitIndicatorCandle?.supertrend2 ?? null : null,
                    exit_supertrend3: isThreeSupertrendMode ? exitIndicatorCandle?.supertrend3 ?? null : null,
                })
                currentPosition = null
                currentEquity += pnl

                if ((isSupertrendMode || isThreeSupertrendMode) && exitReason === 'TREND_REVERSAL') {
                    // Continuous Supertrend behavior: flip side on reversal when all enabled filters pass.
                    if (
                        supertrendFlipSide === 'BUY' &&
                        checkEntryConditions(currentCandle, prevCandle, params, 'BUY', nextCandle)
                    ) {
                        currentPosition = createEntryPosition('BUY', currentCandle, nextCandle, 'SUPERTREND_FLIP_BUY')
                    } else if (
                        supertrendFlipSide === 'SELL' &&
                        checkEntryConditions(currentCandle, prevCandle, params, 'SELL', nextCandle)
                    ) {
                        currentPosition = createEntryPosition('SELL', currentCandle, nextCandle, 'SUPERTREND_FLIP_SELL')
                    }
                }
            }
        } else {
            const canBuy = checkEntryConditions(currentCandle, prevCandle, params, 'BUY', nextCandle)
            const canSell = checkEntryConditions(currentCandle, prevCandle, params, 'SELL', nextCandle)

            if (canBuy) {
                const reason = isSupertrendMode ? 'SUPERTREND_FLIP_BUY' : 'BUY_CONDITIONS_MATCHED'
                currentPosition = createEntryPosition('BUY', currentCandle, nextCandle, reason)
            } else if (canSell) {
                const reason = isSupertrendMode ? 'SUPERTREND_FLIP_SELL' : 'SELL_CONDITIONS_MATCHED'
                currentPosition = createEntryPosition('SELL', currentCandle, nextCandle, reason)
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

function getEnabledIndicatorsSnapshot(params = {}) {
    return {
        timeframe: params.candle_timeframe || null,
        fixtarget: params.fixtarget === true || params.fixtarget === 'true',
        supertrend: Boolean(params.use_supertrend_mode),
        three_supertrend: Boolean(params.use_three_supertrend_mode),
        adx: Boolean(params.adx_enabled),
        rsi: params.rsi_enabled !== false,
        ema1: params.ema1_enabled !== false,
        ema2: params.ema2_enabled !== false,
        macd: Boolean(params.macd_enabled),
        rsi2: Boolean(params.rsi2_timeframe),
        macd2: Boolean(params.macd_2_timeframe),
        macd3: Boolean(params.macd_3_enabled),
        ema3_15m: Boolean(params.ema3_15m_enabled),
        ema4_15m: Boolean(params.ema4_15m_enabled),
    }
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

        const overall = generateBacktestSummary(combinedTrades, combinedEquity, normalizedPayload)
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
    const totalPnL = calculateTradesNetPnl(trades)

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
        momentIST(t.exitTime).diff(momentIST(t.entryTime), 'minutes', true)
    )
    const avgDurationMinutes =
        trades.length > 0 ? durations.reduce((a, b) => a + b, 0) / trades.length : 0
    const averageActiveTimeHours =
        trades.length > 0 ? Number((avgDurationMinutes / 60).toFixed(2)) : 0

    return {
        summary: {
            totalTrades: trades.length,
            winningTrades: winningTrades.length,
            losingTrades: losingTrades.length,
            totalPnL,
            winRate: (trades.length > 0 ? ((winningTrades.length / trades.length) * 100).toFixed(2) : 0) + '%',
            averagePnL: trades.length > 0 ? trades.reduce((a, b) => a + b.pnl, 0) / trades.length : 0,
            averageWinningTrade:
                winningTrades.length > 0 ? winningTrades.reduce((a, b) => a + b.pnl, 0) / winningTrades.length : 0,
            averageLosingTrade:
                losingTrades.length > 0 ? losingTrades.reduce((a, b) => a + b.pnl, 0) / losingTrades.length : 0,
            largestWin: trades.length > 0 ? Math.max(...trades.map((t) => t.pnl)) : 0,
            largestLoss: trades.length > 0 ? Math.min(...trades.map((t) => t.pnl)) : 0,
            maxDrawdown: maxDD,
            averageDurationMinutes: avgDurationMinutes || 0,
            average_active_time: averageActiveTimeHours,
            profitFactor: losingTrades.length > 0 ? Math.abs(
                winningTrades.reduce((a, b) => a + b.pnl, 0) /
                losingTrades.reduce((a, b) => a + b.pnl, 0)
            ).toFixed(2) : (winningTrades.length > 0 ? 'Infinity' : '0.00'),
        },
        enabledIndicators: getEnabledIndicatorsSnapshot(params),
        trades: trades.map((t) => ({
            ...t,
            pnlPercent: ((t.pnl / t.entryPrice) * 100).toFixed(2) + '%',
        })),
    }
}

export default runBacktestStrategy
