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

function resolveStrategyFlags(params = {}) {
    // Centralized feature switches. If a flag is missing in payload,
    // defaults keep legacy behavior (all classic indicators ON, supertrend OFF).
    const readFlag = (key, fallback) =>
        typeof params[key] === 'boolean' ? params[key] : fallback

    return {
        useSupertrend: readFlag('useSupertrend', false),
        useRSI: readFlag('useRSI', true),
        useMACD: readFlag('useMACD', true),
        useEMA: readFlag('useEMA', true),
        useTargetStoploss: readFlag('useTargetStoploss', true),
    }
}

function resolveTimeframeIndicatorSettings(params = {}) {
    // Timeframe + indicator switches (backward-compatible with existing payload keys).
    // You can explicitly disable entire timeframe blocks and/or individual indicators.
    const strategyFlags = resolveStrategyFlags(params)
    const readFlag = (key, fallback) =>
        typeof params[key] === 'boolean' ? params[key] : fallback

    const timeframe3mEnabled = readFlag('timeframe_3m_enabled', true)
    const timeframe15mEnabled = readFlag(
        'timeframe_15m_enabled',
        Boolean(params.rsi2_timeframe || params.macd_2_timeframe || params.ema3_15m_enabled || params.ema4_15m_enabled)
    )
    const timeframe30mEnabled = readFlag(
        'timeframe_30m_enabled',
        Boolean(params.macd_3_timeframe || params.macd_3_enabled)
    )

    const rsi3mEnabled = timeframe3mEnabled && strategyFlags.useRSI && readFlag('rsi_3m_enabled', true)
    const rsi15mEnabled = timeframe15mEnabled && strategyFlags.useRSI && readFlag('rsi_15m_enabled', Boolean(params.rsi2_timeframe))

    const ema3mEnabled = timeframe3mEnabled && strategyFlags.useEMA && readFlag('ema_3m_enabled', true)
    const ema15mFastEnabled = timeframe15mEnabled && strategyFlags.useEMA && readFlag('ema3_15m_enabled', Boolean(params.ema3_15m_enabled))
    const ema15mSlowEnabled = timeframe15mEnabled && strategyFlags.useEMA && readFlag('ema4_15m_enabled', Boolean(params.ema4_15m_enabled))

    const macd3mEnabled = timeframe3mEnabled && strategyFlags.useMACD && readFlag('macd_3m_enabled', Boolean(params.macd_enabled))
    const macd15mEnabled = timeframe15mEnabled && strategyFlags.useMACD && readFlag('macd_15m_enabled', Boolean(params.macd_2_timeframe))
    const macd30mEnabled = timeframe30mEnabled && strategyFlags.useMACD && readFlag('macd_30m_enabled', Boolean(params.macd_3_enabled))

    return {
        timeframe3mEnabled,
        timeframe15mEnabled,
        timeframe30mEnabled,
        rsi3mEnabled,
        rsi15mEnabled,
        ema3mEnabled,
        ema15mFastEnabled,
        ema15mSlowEnabled,
        macd3mEnabled,
        macd15mEnabled,
        macd30mEnabled,
    }
}

function normalizeTrendPayload(raw = {}) {
    // Accept the new tf_one/tf_two/tf_three schema and map it to
    // existing internal fields so the rest of the backtest engine stays unchanged.
    const payload = { ...raw }
    const normalized = { ...payload }

    const pick = (...keys) => {
        for (const key of keys) {
            if (payload[key] !== undefined) return payload[key]
        }
        return undefined
    }

    // Base timeframe (tf_one)
    normalized.candle_timeframe = pick('candle_timeframe', 'tf_timeframe_one', 'tf_timeframe_1') || 'THREE_MINUTE'
    if (pick('tf_one_isrsi') !== undefined) normalized.rsi_3m_enabled = Boolean(pick('tf_one_isrsi'))
    if (pick('tf_one_rsi_length') !== undefined) normalized.rsi_length = Number(pick('tf_one_rsi_length'))
    if (pick('tf_one_rsi_buy_level') !== undefined) normalized.rsi_buy_level = Number(pick('tf_one_rsi_buy_level'))
    if (pick('tf_one_rsi_sell_level') !== undefined) normalized.rsi_sell_level = Number(pick('tf_one_rsi_sell_level'))

    if (pick('tf_one_ismacd') !== undefined) {
        normalized.macd_3m_enabled = Boolean(pick('tf_one_ismacd'))
        normalized.macd_enabled = Boolean(pick('tf_one_ismacd'))
    }
    if (pick('tf_one_macd_fast') !== undefined) normalized.macd_fast = Number(pick('tf_one_macd_fast'))
    if (pick('tf_one_macd_slow') !== undefined) normalized.macd_slow = Number(pick('tf_one_macd_slow'))
    if (pick('tf_one_macd_signal') !== undefined) normalized.macd_signal = Number(pick('tf_one_macd_signal'))

    if (pick('tf_one_isema1', 'tf_one_isema2') !== undefined) {
        normalized.ema_3m_enabled = Boolean(pick('tf_one_isema1')) || Boolean(pick('tf_one_isema2'))
    }
    if (pick('tf_one_ema1') !== undefined) normalized.ema1_length = Number(pick('tf_one_ema1'))
    if (pick('tf_one_ema2') !== undefined) normalized.ema2_length = Number(pick('tf_one_ema2'))

    // Supertrend flags per slot:
    // tf_one_useSupertrend drives live entry/exit behavior.
    // tf_two/tf_three flags are accepted for config consistency and future extension.
    if (pick('tf_one_useSupertrend') !== undefined) {
        normalized.tf_one_useSupertrend = Boolean(pick('tf_one_useSupertrend'))
    }
    if (pick('tf_two_useSupertrend') !== undefined) {
        normalized.tf_two_useSupertrend = Boolean(pick('tf_two_useSupertrend'))
    }
    if (pick('tf_three_useSupertrend') !== undefined) {
        normalized.tf_three_useSupertrend = Boolean(pick('tf_three_useSupertrend'))
    }

    // Backward compatibility:
    // - Prefer tf_one_useSupertrend
    // - Fallback to global useSupertrend
    if (normalized.tf_one_useSupertrend !== undefined) {
        normalized.useSupertrend = normalized.tf_one_useSupertrend
    } else if (pick('useSupertrend') !== undefined) {
        normalized.useSupertrend = Boolean(pick('useSupertrend'))
        normalized.tf_one_useSupertrend = normalized.useSupertrend
    }

    // Supertrend period/factor priority:
    // 1) tf_one_* when tf_one_useSupertrend is true
    // 2) tf_two_* when tf_two_useSupertrend is true
    // 3) tf_three_* when tf_three_useSupertrend is true
    // 4) fallback to any provided value for backward compatibility
    let stLen
    let stFactor
    if (normalized.tf_one_useSupertrend) {
        stLen = pick('tf_one_supertrend_length')
        stFactor = pick('tf_one_supertrend_factor')
    }
    if ((stLen === undefined || stFactor === undefined) && normalized.tf_two_useSupertrend) {
        stLen = stLen === undefined ? pick('tf_two_supertrend_length') : stLen
        stFactor = stFactor === undefined ? pick('tf_two_supertrend_factor') : stFactor
    }
    if ((stLen === undefined || stFactor === undefined) && normalized.tf_three_useSupertrend) {
        stLen = stLen === undefined ? pick('tf_three_supertrend_length') : stLen
        stFactor = stFactor === undefined ? pick('tf_three_supertrend_factor') : stFactor
    }
    if (stLen === undefined) {
        stLen = pick('tf_one_supertrend_length', 'tf_two_supertrend_length', 'tf_three_supertrend_length')
    }
    if (stFactor === undefined) {
        stFactor = pick('tf_one_supertrend_factor', 'tf_two_supertrend_factor', 'tf_three_supertrend_factor')
    }
    if (stLen !== undefined) normalized.supertrend_period = Number(stLen)
    if (stFactor !== undefined) normalized.supertrend_multiplier = Number(stFactor)

    // 15m (tf_two)
    const tfTwo = pick('tf_timeframe_two', 'rsi2_timeframe', 'macd_2_timeframe') || 'FIFTEEN_MINUTE'
    normalized.tf_timeframe_two = tfTwo
    if (pick('timeframe_15m_enabled') !== undefined) normalized.timeframe_15m_enabled = Boolean(pick('timeframe_15m_enabled'))

    if (pick('tf_two_isrsi') !== undefined) normalized.rsi_15m_enabled = Boolean(pick('tf_two_isrsi'))
    if (pick('tf_two_rsi_length') !== undefined) normalized.rsi2_length = Number(pick('tf_two_rsi_length'))
    if (pick('tf_two_rsi_buy_level') !== undefined) normalized.rsi2_buy_level = Number(pick('tf_two_rsi_buy_level'))
    if (pick('tf_two_rsi_sell_level') !== undefined) normalized.rsi2_sell_level = Number(pick('tf_two_rsi_sell_level'))
    if (normalized.rsi_15m_enabled) normalized.rsi2_timeframe = tfTwo

    if (pick('tf_two_ismacd') !== undefined) normalized.macd_15m_enabled = Boolean(pick('tf_two_ismacd'))
    if (pick('tf_two_macd_fast') !== undefined) normalized.macd_2_fast = Number(pick('tf_two_macd_fast'))
    if (pick('tf_two_macd_slow') !== undefined) normalized.macd_2_slow = Number(pick('tf_two_macd_slow'))
    if (pick('tf_two_macd_signal') !== undefined) normalized.macd_2_signal = Number(pick('tf_two_macd_signal'))
    if (normalized.macd_15m_enabled) normalized.macd_2_timeframe = tfTwo

    if (pick('tf_two_isema1') !== undefined) normalized.ema3_15m_enabled = Boolean(pick('tf_two_isema1'))
    if (pick('tf_two_isema2') !== undefined) normalized.ema4_15m_enabled = Boolean(pick('tf_two_isema2'))
    if (pick('tf_two_ema1') !== undefined) normalized.ema3_15m = Number(pick('tf_two_ema1'))
    if (pick('tf_two_ema2') !== undefined) normalized.ema4_15m = Number(pick('tf_two_ema2'))

    // 30m (tf_three)
    const tfThree = pick('tf_timeframe_three', 'macd_3_timeframe') || 'THIRTY_MINUTE'
    normalized.tf_timeframe_three = tfThree
    if (pick('timeframe_30m_enabled') !== undefined) normalized.timeframe_30m_enabled = Boolean(pick('timeframe_30m_enabled'))

    if (pick('tf_three_ismacd') !== undefined) {
        normalized.macd_30m_enabled = Boolean(pick('tf_three_ismacd'))
        normalized.macd_3_enabled = Boolean(pick('tf_three_ismacd'))
    }
    if (pick('tf_three_macd_fast') !== undefined) normalized.macd_3_fast = Number(pick('tf_three_macd_fast'))
    if (pick('tf_three_macd_slow') !== undefined) normalized.macd_3_slow = Number(pick('tf_three_macd_slow'))
    if (pick('tf_three_macd_signal') !== undefined) normalized.macd_3_signal = Number(pick('tf_three_macd_signal'))
    if (normalized.macd_30m_enabled) normalized.macd_3_timeframe = tfThree

    // Global switches (single key in valid JSON)
    if (pick('useTargetStoploss') !== undefined) normalized.useTargetStoploss = Boolean(pick('useTargetStoploss'))

    // Keep explicit timeframe_3m_enabled if provided.
    if (pick('timeframe_3m_enabled') !== undefined) normalized.timeframe_3m_enabled = Boolean(pick('timeframe_3m_enabled'))

    return normalized
}

function calculateSupertrendSeries(candles, period = 10, multiplier = 3) {
    // Returns two aligned arrays:
    // 1) supertrend value
    // 2) trend direction (1 = green/uptrend, -1 = red/downtrend)
    const n = candles.length
    const supertrend = Array(n).fill(null)
    const direction = Array(n).fill(null)
    if (!n || period <= 0 || multiplier <= 0) {
        return { supertrend, direction }
    }

    const tr = Array(n).fill(null)
    const atr = Array(n).fill(null)

    for (let i = 0; i < n; i++) {
        // True Range for ATR
        const c = candles[i]
        const high = Number(c.high)
        const low = Number(c.low)
        const prevClose = i > 0 ? Number(candles[i - 1].close) : Number(c.close)
        tr[i] = Math.max(
            high - low,
            Math.abs(high - prevClose),
            Math.abs(low - prevClose)
        )
    }

    if (n < period) {
        return { supertrend, direction }
    }

    let trSum = 0
    for (let i = 0; i < period; i++) trSum += tr[i]
    atr[period - 1] = trSum / period
    for (let i = period; i < n; i++) {
        atr[i] = ((atr[i - 1] * (period - 1)) + tr[i]) / period
    }

    let prevFinalUpper = null
    let prevFinalLower = null
    let prevDirection = null

    for (let i = period - 1; i < n; i++) {
        const c = candles[i]
        const high = Number(c.high)
        const low = Number(c.low)
        const close = Number(c.close)
        const prevClose = i > 0 ? Number(candles[i - 1].close) : close
        const hl2 = (high + low) / 2
        const basicUpper = hl2 + (multiplier * atr[i])
        const basicLower = hl2 - (multiplier * atr[i])

        let finalUpper = basicUpper
        let finalLower = basicLower

        if (prevFinalUpper != null && prevFinalLower != null) {
            // Final bands are "sticky" to avoid frequent band jumps.
            finalUpper = (basicUpper < prevFinalUpper || prevClose > prevFinalUpper)
                ? basicUpper
                : prevFinalUpper
            finalLower = (basicLower > prevFinalLower || prevClose < prevFinalLower)
                ? basicLower
                : prevFinalLower
        }

        let currentDirection = prevDirection
        // Trend flip rules:
        // close above upper band => uptrend
        // close below lower band => downtrend
        if (prevDirection == null) {
            currentDirection = close <= finalUpper ? -1 : 1
        } else if (close > finalUpper) {
            currentDirection = 1
        } else if (close < finalLower) {
            currentDirection = -1
        }

        direction[i] = currentDirection
        supertrend[i] = currentDirection === 1 ? finalLower : finalUpper

        prevFinalUpper = finalUpper
        prevFinalLower = finalLower
        prevDirection = currentDirection
    }

    return { supertrend, direction }
}

function timeframeToMinutes(tf) {
    if (typeof tf === 'number') return tf
    if (!tf) return null
    const key = String(tf).toUpperCase()
    const numericMatch = key.match(/^(\d+)\s*(M|MIN|MINS|MINUTE|MINUTES)?$/)
    if (numericMatch) return Number(numericMatch[1])
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

function validatePayload(payload) {
    const flags = resolveStrategyFlags(payload)
    const tf = resolveTimeframeIndicatorSettings(payload)
    // Require only what is needed for enabled modules.
    const required = [
        'instrument',
        'candle_timeframe',
        'from_date',
        'to_date',
    ]
    if (tf.rsi3mEnabled) {
        required.push('rsi_length', 'rsi_buy_level', 'rsi_sell_level')
    }
    if (tf.ema3mEnabled) {
        required.push('ema1_length', 'ema2_length')
    }
    if (tf.rsi15mEnabled) {
        required.push('rsi2_timeframe', 'rsi2_length')
    }
    if (tf.ema15mFastEnabled) required.push('ema3_15m')
    if (tf.ema15mSlowEnabled) required.push('ema4_15m')
    if (tf.macd3mEnabled) required.push('macd_fast', 'macd_slow', 'macd_signal')
    if (tf.macd15mEnabled) required.push('macd_2_timeframe', 'macd_2_fast', 'macd_2_slow', 'macd_2_signal')
    if (tf.macd30mEnabled) required.push('macd_3_timeframe', 'macd_3_fast', 'macd_3_slow', 'macd_3_signal')
    if (flags.useTargetStoploss) {
        required.push('target_points', 'stoploss_points')
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
    if (tf.rsi3mEnabled) numericFields.push('rsi_length', 'rsi_buy_level', 'rsi_sell_level')
    if (tf.ema3mEnabled) numericFields.push('ema1_length', 'ema2_length')
    if (flags.useTargetStoploss) numericFields.push('target_points', 'stoploss_points')
    if (tf.rsi15mEnabled) numericFields.push('rsi2_length')
    if (tf.ema15mFastEnabled) numericFields.push('ema3_15m')
    if (tf.ema15mSlowEnabled) numericFields.push('ema4_15m')
    if (tf.macd3mEnabled) numericFields.push('macd_fast', 'macd_slow', 'macd_signal')
    if (tf.macd15mEnabled) numericFields.push('macd_2_fast', 'macd_2_slow', 'macd_2_signal')
    if (tf.macd30mEnabled) numericFields.push('macd_3_fast', 'macd_3_slow', 'macd_3_signal')
    if (flags.useSupertrend) {
        if (payload.supertrend_period != null) numericFields.push('supertrend_period')
        if (payload.supertrend_multiplier != null) numericFields.push('supertrend_multiplier')
    }

    numericFields.forEach((field) => {
        if (
            field !== 'candle_timeframe' &&
            (typeof payload[field] !== 'number' || payload[field] <= 0)
        ) {
            throw new Error(`${field} must be a positive number`)
        }
    })

    if (tf.macd3mEnabled && payload.macd_fast >= payload.macd_slow) {
        throw new Error('macd_fast period must be less than macd_slow period')
    }
    if (tf.macd15mEnabled && payload.macd_2_fast >= payload.macd_2_slow) {
        throw new Error('macd_2_fast period must be less than macd_2_slow period')
    }
    if (tf.macd30mEnabled && payload.macd_3_fast >= payload.macd_3_slow) {
        throw new Error('macd_3_fast period must be less than macd_3_slow period')
    }

    if (payload.adx_enabled) {
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
    const flags = resolveStrategyFlags(params)
    const tf = resolveTimeframeIndicatorSettings(params)
    // RSI needs rsi_length periods
    let warmup = tf.rsi3mEnabled ? params.rsi_length : 20

    // EMA needs roughly 2.5 times the period for proper convergence
    if (tf.ema3mEnabled) {
        warmup = Math.max(warmup, Math.ceil(2.5 * params.ema1_length))
        warmup = Math.max(warmup, Math.ceil(2.5 * params.ema2_length))
    }

    if (tf.ema15mFastEnabled) {
        warmup = Math.max(warmup, Math.ceil(2.5 * params.ema3_15m))
    }

    if (tf.ema15mSlowEnabled) {
        warmup = Math.max(warmup, Math.ceil(2.5 * params.ema4_15m))
    }


    // MACD needs the longer of fast/slow periods plus signal
    if (tf.macd3mEnabled) {
        warmup = Math.max(warmup, params.macd_slow + params.macd_signal)
    }

    if (flags.useSupertrend) {
        const stPeriod = Number(params.supertrend_period) > 0 ? Number(params.supertrend_period) : 10
        warmup = Math.max(warmup, stPeriod * 3)
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
    const tf = resolveTimeframeIndicatorSettings(params)
    // For properly matching old results, we need enough warmup for the SLOWEST timeframe.
    // If we need 100 candles of 30m, that's 3000 minutes.
    const maxTfMinutes = params.candle_timeframe_number || 3;
    const htf30 = 30; // Max HTF we usually care about
    const minimumWarmupCandles = 100;

    const extraCandlesNeeded = Math.max(
        (minimumWarmupCandles + (tf.rsi3mEnabled ? params.rsi_length : 0)) * (htf30 / maxTfMinutes),
        (Math.ceil(2.5 * (tf.ema3mEnabled ? params.ema1_length : 10)) + minimumWarmupCandles) * (htf30 / maxTfMinutes),
        (tf.macd30mEnabled) ? (params.macd_3_slow + params.macd_3_signal) * 2 * (30 / maxTfMinutes) + (100 * (30 / maxTfMinutes)) : 0
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
    const tf = resolveTimeframeIndicatorSettings(params)

    // Sort candles by timestamp first to ensure correct order
    candles.sort(
        (a, b) => momentIST(a.timestamp).valueOf() - momentIST(b.timestamp).valueOf()
    )

    const closes = candles.map((c) => c.close)
    // console.log({ length: closes.length, closes })

    // Calculate RSI only when enabled. Otherwise keep null values downstream.
    let rsi = [[]]
    if (tf.rsi3mEnabled) {
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

    // Calculate EMA only when enabled.
    let ema1Result = [[]]
    let ema2Result = [[]]
    if (tf.ema3mEnabled) {
        ;[ema1Result, ema2Result] = await Promise.all([
            new Promise((resolve, reject) => {
                tulind.indicators.ema.indicator(
                    [closes],
                    [params.ema1_length],
                    (err, results) => {
                        if (err) return reject(err)
                        resolve(results)
                    }
                )
            }),
            new Promise((resolve, reject) => {
                tulind.indicators.ema.indicator(
                    [closes],
                    [params.ema2_length],
                    (err, results) => {
                        if (err) return reject(err)
                        resolve(results)
                    }
                )
            }),
        ])
    }

    // Calculate MACD only when enabled.
    let macd = [],
        macdSignal = [],
        macdHistogram = []
    if (tf.macd3mEnabled) {
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

    // Map sliced results onto the post-start candles
    const round = (val, decimals = 2) =>
        val != null ? Number(val.toFixed(decimals)) : null;

    const stPeriod = Number(params.supertrend_period) > 0 ? Number(params.supertrend_period) : 10
    const stMultiplier = Number(params.supertrend_multiplier) > 0 ? Number(params.supertrend_multiplier) : 3
    // Supertrend is always computed so it is available whenever useSupertrend=true.
    const { supertrend, direction } = calculateSupertrendSeries(postStartCandles, stPeriod, stMultiplier)

    const candleData = postStartCandles.map((c, idx) => ({
        ...c,
        rsi: tf.rsi3mEnabled ? round(slicedRsi[idx], 2) : null,
        ema1: tf.ema3mEnabled ? round(slicedEma1[idx], 2) : null,
        ema2: tf.ema3mEnabled ? round(slicedEma2[idx], 2) : null,
        macd: tf.macd3mEnabled ? round(slicedMacd[idx], 2) : null,
        macdSignal: tf.macd3mEnabled ? round(slicedMacdSignal[idx], 2) : null,
        macdHistogram: tf.macd3mEnabled ? round(slicedMacdHist[idx], 2) : null,
        bbUpper: params.bb_enabled ? round(slicedBbUpper[idx], 2) : null,
        bbMiddle: params.bb_enabled ? round(slicedBbMiddle[idx], 2) : null,
        bbLower: params.bb_enabled ? round(slicedBbLower[idx], 2) : null,
        supertrend: round(supertrend[idx], 2),
        supertrendDirection: direction[idx],
    }));

    return candleData
}

function checkEntryConditions(currentCandle, prevCandle, params, side = 'BUY') {
    const time = momentIST(currentCandle.timestamp)
    const tf = resolveTimeframeIndicatorSettings(params)

    // console.log(currentCandle)

    // Time gate intentionally disabled for this trend backtest.
    // if (time.format('HH:mm') < '09:30' || time.format('HH:mm') >= '15:10') {
    //     return false
    // }

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
    const macdBuyOk =
        (!tf.macd3mEnabled || currentCandle.macdSignal < currentCandle.macd) &&
        (!tf.macd15mEnabled ||
            (currentCandle.macd2 !== null &&
                currentCandle.macd2Signal !== null &&
                currentCandle.macd2Signal < currentCandle.macd2)) &&
        (!tf.macd30mEnabled ||
            (currentCandle.macd3 !== null &&
                currentCandle.macd3Signal !== null &&
                currentCandle.macd3Signal < currentCandle.macd3))
    const macdSellOk =
        (!tf.macd3mEnabled || currentCandle.macdSignal > currentCandle.macd) &&
        (!tf.macd15mEnabled ||
            (currentCandle.macd2 !== null &&
                currentCandle.macd2Signal !== null &&
                currentCandle.macd2Signal > currentCandle.macd2)) &&
        (!tf.macd30mEnabled ||
            (currentCandle.macd3 !== null &&
                currentCandle.macd3Signal !== null &&
                currentCandle.macd3Signal > currentCandle.macd3))

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
            // If useRSI/useEMA/useMACD are false, that block is skipped.
            (!tf.rsi3mEnabled || (
                currentCandle.rsi > params.rsi_buy_level &&
                prevCandle.rsi < params.rsi_buy_level &&
                (!tf.rsi15mEnabled || (rsi2Buy === null || (rsi2 !== null && rsi2 >= rsi2Buy)))
            )) &&
            (!tf.ema3mEnabled || (
                currentCandle.close > currentCandle.ema1 &&
                currentCandle.close > currentCandle.ema2 &&
                (!tf.ema15mFastEnabled || currentCandle.candle15m.close > currentCandle.candle15m.ema3_15m) &&
                (!tf.ema15mSlowEnabled || currentCandle.candle15m.close > currentCandle.candle15m.ema4_15m)
            )) &&
            // (!params.vwap_timeframe || currentCandle.close > currentCandle.vwap) &&
            macdBuyOk

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
            (!tf.rsi3mEnabled || (
                currentCandle.rsi < params.rsi_sell_level &&
                prevCandle.rsi > params.rsi_sell_level &&
                (!tf.rsi15mEnabled || (rsi2Sell === null || (rsi2 !== null && rsi2 <= rsi2Sell)))
            )) &&
            (!tf.ema3mEnabled || (
                currentCandle.close < currentCandle.ema1 &&
                currentCandle.close < currentCandle.ema2 &&
                (!tf.ema15mFastEnabled || currentCandle.candle15m.close < currentCandle.candle15m.ema3_15m) &&
                (!tf.ema15mSlowEnabled || currentCandle.candle15m.close < currentCandle.candle15m.ema4_15m)
            )) &&
            // (!params.vwap_timeframe || currentCandle.close < currentCandle.vwap) &&
            macdSellOk

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
    const flags = resolveStrategyFlags(params)
    const debugEach3mClose = params.debug_each_3m_close !== false
    const n = (v) => (v == null || Number.isNaN(Number(v)) ? null : Number(Number(v).toFixed(2)))
    const getDir = (candle) => (typeof candle?.supertrendDirection === 'number' ? candle.supertrendDirection : null)
    // Keep position object shape consistent with existing response structure.
    const buildPosition = (side, entryPrice, entryTime, candle) => ({
        entryPrice,
        entryTime,
        side,
        indicators: {
            rsi: candle.rsi,
            rsi2: candle.rsi2,
            ema1: candle.ema1,
            ema2: candle.ema2,
            macd: candle.macd,
            macdSignal: candle.macdSignal,
            macdHistogram: candle.macdHistogram,
            macd2: candle.macd2,
            macd2Signal: candle.macd2Signal,
            macd2Histogram: candle.macd2Histogram,
            macd3: candle.macd3,
            macd3Signal: candle.macd3Signal,
            macd3Histogram: candle.macd3Histogram,
            supertrend: candle.supertrend,
            supertrendDirection: candle.supertrendDirection,
        },
    })

    console.log(`[Backtest] Starting simulation on ${candles.length} candles...`)

    for (let i = 1; i < candles.length; i++) {
        // Progress log every 1% or every 1000 candles
        if (i % Math.max(1, Math.floor(candles.length / 100)) === 0 || i === candles.length - 1) {
            const progress = (((i + 1) / candles.length) * 100).toFixed(1);
            process.stdout.write(`\r[Backtest] Progress: ${progress}% (${i + 1}/${candles.length})`);
        }

        const currentCandle = candles[i]
        const prevCandle = candles[i - 1]
        const prevPrevCandle = i > 1 ? candles[i - 2] : null
        // "Next candle open" execution:
        // Detect trend change on previous closed candle(s), then act on current candle open.
        const supertrendFlipToBuy = flags.useSupertrend && prevPrevCandle && getDir(prevPrevCandle) === -1 && getDir(prevCandle) === 1
        const supertrendFlipToSell = flags.useSupertrend && prevPrevCandle && getDir(prevPrevCandle) === 1 && getDir(prevCandle) === -1
        const time = momentIST(currentCandle.timestamp)

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

            if (time.format('HH:mm') === '15:25') {
                exitReason = 'FORCED_EXIT'
                exitPrice = currentCandle.close
            } else if (flags.useSupertrend) {
                // When Supertrend mode is ON, reversal is the primary exit signal.
                if (currentPosition.side === 'BUY' && supertrendFlipToSell) {
                    exitReason = 'SUPER_TREND_REVERSAL'
                    exitPrice = currentCandle.open
                } else if (currentPosition.side === 'SELL' && supertrendFlipToBuy) {
                    exitReason = 'SUPER_TREND_REVERSAL'
                    exitPrice = currentCandle.open
                }
            }

            if (!exitReason && flags.useTargetStoploss) {
                // Optional target/stoploss module.
                if (currentPosition.side === 'BUY') {
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
            }

            if (exitReason) {
                const finalExitPrice = exitPrice != null ? exitPrice : currentCandle.close
                const pnl = currentPosition.side === 'BUY'
                    ? finalExitPrice - currentPosition.entryPrice
                    : currentPosition.entryPrice - finalExitPrice

                trades.push({
                    ...currentPosition,
                    exitPrice: finalExitPrice,
                    exitTime: currentCandle.timestamp,
                    pnl,
                    exitReason,
                })
                currentPosition = null
                currentEquity += pnl
            }
        } else {
            if (flags.useSupertrend) {
                // Supertrend fully controls entries when enabled.
                if (supertrendFlipToBuy) {
                    currentPosition = buildPosition('BUY', currentCandle.open, currentCandle.timestamp, currentCandle)
                } else if (supertrendFlipToSell) {
                    currentPosition = buildPosition('SELL', currentCandle.open, currentCandle.timestamp, currentCandle)
                }
            } else if (checkEntryConditions(currentCandle, prevCandle, params, 'BUY')) {
                currentPosition = buildPosition('BUY', currentCandle.close, currentCandle.timestamp, currentCandle)
            } else if (checkEntryConditions(currentCandle, prevCandle, params, 'SELL')) {
                currentPosition = buildPosition('SELL', currentCandle.close, currentCandle.timestamp, currentCandle)
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
        const normalizedBody = normalizeTrendPayload(req.body || {})
        console.log(`[Backtest] Start request for ${normalizedBody.instrument} from ${normalizedBody.from_date} to ${normalizedBody.to_date}`);

        validatePayload(normalizedBody)
        const tf = resolveTimeframeIndicatorSettings(normalizedBody)
        // Respect explicit timeframe switches (3m/15m/30m) and per-timeframe indicators.
        const originalStartDate = normalizedBody.from_date

        // Fetch BASE historical data (3m candles)
        // We ensure enough warmup by adding extra days to the base fetch
        const baseTimeframe = normalizedBody.candle_timeframe || '3m';
        const baseTfMin = timeframeToMinutes(baseTimeframe) || 3;

        // Calculate needed warmup in days based on the longest indicator/timeframe
        // Slot-based timeframe resolution: TF2/TF3 can be any minute bucket (e.g. 5, 15, 45).
        const slot2TfMin = tf.timeframe15mEnabled
            ? timeframeToMinutes(normalizedBody.tf_timeframe_two || normalizedBody.rsi2_timeframe || normalizedBody.macd_2_timeframe)
            : null
        const slot3TfMin = tf.timeframe30mEnabled
            ? timeframeToMinutes(normalizedBody.tf_timeframe_three || normalizedBody.macd_3_timeframe)
            : null
        if (tf.timeframe15mEnabled && !slot2TfMin) {
            throw new Error('Invalid tf_timeframe_two (or rsi2_timeframe/macd_2_timeframe)')
        }
        if (tf.timeframe30mEnabled && !slot3TfMin) {
            throw new Error('Invalid tf_timeframe_three (or macd_3_timeframe)')
        }
        const maxHtfMin = Math.max(
            tf.rsi15mEnabled ? (timeframeToMinutes(normalizedBody.rsi2_timeframe) || 0) : 0,
            tf.macd15mEnabled ? (timeframeToMinutes(normalizedBody.macd_2_timeframe) || 0) : 0,
            tf.macd30mEnabled ? (timeframeToMinutes(normalizedBody.macd_3_timeframe) || 0) : 0,
            (tf.ema15mFastEnabled || tf.ema15mSlowEnabled) ? (slot2TfMin || 0) : 0,
            15, 30 // hardcoded for 15m/30m Indicators
        );

        const warmupMultiplier = Math.ceil(maxHtfMin / baseTfMin);
        const reqWithWarmup = { ...normalizedBody, candle_timeframe_number: baseTfMin };

        // 1. Fetch all data ONCE using base timeframe
        const allCandles = await fetchHistoricalData(reqWithWarmup);
        if (!allCandles.length) throw new Error('No historical data available');

        // 2. Resample slot timeframes from allCandles locally
        console.log(`[Backtest] Resampling dynamic slot timeframes locally...`);
        const candlesTf2 = tf.timeframe15mEnabled ? resampleCandles(allCandles, slot2TfMin) : [];
        const candlesTf3 = tf.timeframe30mEnabled ? resampleCandles(allCandles, slot3TfMin) : [];

        const rsi2TfMin = tf.rsi15mEnabled ? timeframeToMinutes(normalizedBody.rsi2_timeframe) : null;
        const macd2TfMin = tf.macd15mEnabled ? timeframeToMinutes(normalizedBody.macd_2_timeframe) : null;
        const macd3TfMin = tf.macd30mEnabled ? timeframeToMinutes(normalizedBody.macd_3_timeframe) : null;

        // 3. Process base candles and calculate indicators
        console.log(`[Backtest] Calculating base indicators...`);
        const processedBaseCandles = await calculateIndicators(allCandles, normalizedBody);

        // 4. Construct Rolling HTF Snapshots for configured slots.
        console.log(`[Backtest] Constructing snapshots for configured TF2/TF3...`);

        // Map TF2 Snapshot Indicators
        const rollingTf2 = tf.timeframe15mEnabled
            ? buildRollingHTFMap(processedBaseCandles, slot2TfMin, candlesTf2, {
                emaPeriods: {
                    ema3_15m: tf.ema15mFastEnabled ? normalizedBody.ema3_15m : null,
                    ema4_15m: tf.ema15mSlowEnabled ? normalizedBody.ema4_15m : null,
                },
                rsiPeriod: (tf.rsi15mEnabled && rsi2TfMin === slot2TfMin) ? normalizedBody.rsi2_length : null,
                macd2Config: (tf.macd15mEnabled && macd2TfMin === slot2TfMin) ? {
                    fast: normalizedBody.macd_2_fast, slow: normalizedBody.macd_2_slow, signal: normalizedBody.macd_2_signal
                } : null,
                macd3Config: (tf.macd30mEnabled && macd3TfMin === slot2TfMin) ? {
                    fast: normalizedBody.macd_3_fast, slow: normalizedBody.macd_3_slow, signal: normalizedBody.macd_3_signal
                } : null
            })
            : new Map();

        // Map TF3 Snapshot Indicators
        const rollingTf3 = tf.timeframe30mEnabled
            ? buildRollingHTFMap(processedBaseCandles, slot3TfMin, candlesTf3, {
                rsiPeriod: (tf.rsi15mEnabled && rsi2TfMin === slot3TfMin) ? normalizedBody.rsi2_length : null,
                macd2Config: (tf.macd15mEnabled && macd2TfMin === slot3TfMin) ? {
                    fast: normalizedBody.macd_2_fast, slow: normalizedBody.macd_2_slow, signal: normalizedBody.macd_2_signal
                } : null,
                macd3Config: (tf.macd30mEnabled && macd3TfMin === slot3TfMin) ? {
                    fast: normalizedBody.macd_3_fast, slow: normalizedBody.macd_3_slow, signal: normalizedBody.macd_3_signal
                } : null
            })
            : new Map();

        const round = (val, decimals = 2) => val != null ? Number(val.toFixed(decimals)) : null;

        // 4. Final mapping of snapshots to 3m candles
        processedBaseCandles.forEach((c) => {
            const snapTf2 = rollingTf2.get(c.timestamp);
            const snapTf3 = rollingTf3.get(c.timestamp);

            if (snapTf2) {
                c.candle15m = {
                    ...snapTf2.candle,
                    ema3_15m: round(snapTf2.emaValues?.ema3_15m, 2),
                    ema4_15m: round(snapTf2.emaValues?.ema4_15m, 2),
                };
                if (tf.rsi15mEnabled && rsi2TfMin === slot2TfMin) c.rsi2 = round(snapTf2.rsi, 2);
                if (tf.macd15mEnabled && macd2TfMin === slot2TfMin && snapTf2.macd2) {
                    c.macd2 = round(snapTf2.macd2.MACD, 2);
                    c.macd2Signal = round(snapTf2.macd2.signal, 2);
                    c.macd2Histogram = round(snapTf2.macd2.histogram, 2);
                }
                if (tf.macd30mEnabled && macd3TfMin === slot2TfMin && snapTf2.macd3) {
                    c.macd3 = round(snapTf2.macd3.MACD, 2);
                    c.macd3Signal = round(snapTf2.macd3.signal, 2);
                    c.macd3Histogram = round(snapTf2.macd3.histogram, 2);
                }
            }

            if (snapTf3) {
                c.candle30m = { ...snapTf3.candle };
                if (tf.rsi15mEnabled && rsi2TfMin === slot3TfMin) c.rsi2 = round(snapTf3.rsi, 2);
                if (tf.macd15mEnabled && macd2TfMin === slot3TfMin && snapTf3.macd2) {
                    c.macd2 = round(snapTf3.macd2.MACD, 2);
                    c.macd2Signal = round(snapTf3.macd2.signal, 2);
                    c.macd2Histogram = round(snapTf3.macd2.histogram, 2);
                }
                if (tf.macd30mEnabled && macd3TfMin === slot3TfMin && snapTf3.macd3) {
                    c.macd3 = round(snapTf3.macd3.MACD, 2);
                    c.macd3Signal = round(snapTf3.macd3.signal, 2);
                    c.macd3Histogram = round(snapTf3.macd3.histogram, 2);
                }
            }
        });

        const results = await runBacktest(processedBaseCandles, normalizedBody);
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
        const normalizedBody = normalizeTrendPayload(req.body || {})
        validatePayload(normalizedBody)

        const from = momentIST(normalizedBody.from_date).startOf('day')
        const to = momentIST(normalizedBody.to_date).startOf('day')
        const monthlyRanges = splitRangeMonthWise(normalizedBody.from_date, normalizedBody.to_date)
        const hasAtLeastOneFullMonth = from.clone().add(1, 'month').isSameOrBefore(to, 'day')
        const shouldRunMonthWise = monthlyRanges.length > 1 && hasAtLeastOneFullMonth

        if (!shouldRunMonthWise) {
            const singleResult = await runExistingBacktestAndCapture(normalizedBody)
            printBacktestConsoleSummary(
                `${from.format('YYYY-MM-DD')} to ${to.format('YYYY-MM-DD')}`,
                singleResult
            )
            return res.json(singleResult)
        }

        const monthlyResults = []
        for (const range of monthlyRanges) {
            const result = await runExistingBacktestAndCapture({
                ...normalizedBody,
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

        const overall = generateBacktestSummary(combinedTrades, combinedEquity, normalizedBody)
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


/*
{
  "instrument": "99926000",
  "from_date": "2025-01-01",
  "to_date": "2025-01-31",

  "useTargetStoploss": true,
  "target_points": 25,
  "stoploss_points": 25,

  "timeframe_3m_enabled": true,
  "tf_timeframe_one": "THREE_MINUTE",

  "tf_one_isrsi": true,
  "tf_one_rsi_length": 14,
  "tf_one_rsi_buy_level": 60,
  "tf_one_rsi_sell_level": 40,

  "tf_one_ismacd": true,
  "tf_one_macd_fast": 5,
  "tf_one_macd_slow": 10,
  "tf_one_macd_signal": 5,

  "tf_one_isema1": true,
  "tf_one_ema1": 9,
  "tf_one_isema2": true,
  "tf_one_ema2": 21,

  "tf_one_useSupertrend": false,
  "tf_one_supertrend_length": 10,
  "tf_one_supertrend_factor": 3,

  "timeframe_15m_enabled": true,
  "tf_timeframe_two": "FIFTEEN_MINUTE",

  "tf_two_isrsi": true,
  "tf_two_rsi_length": 14,
  "tf_two_rsi_buy_level": 60,
  "tf_two_rsi_sell_level": 40,

  "tf_two_ismacd": true,
  "tf_two_macd_fast": 12,
  "tf_two_macd_slow": 24,
  "tf_two_macd_signal": 9,

  "tf_two_isema1": false,
  "tf_two_ema1": 9,
  "tf_two_isema2": false,
  "tf_two_ema2": 21,

  "tf_two_useSupertrend": false,
  "tf_two_supertrend_length": 10,
  "tf_two_supertrend_factor": 3,

  "timeframe_30m_enabled": true,
  "tf_timeframe_three": "THIRTY_MINUTE",

  "tf_three_isrsi": false,
  "tf_three_rsi_length": 14,
  "tf_three_rsi_buy_level": 60,
  "tf_three_rsi_sell_level": 40,

  "tf_three_ismacd": true,
  "tf_three_macd_fast": 5,
  "tf_three_macd_slow": 10,
  "tf_three_macd_signal": 5,

  "tf_three_isema1": false,
  "tf_three_ema1": 9,
  "tf_three_isema2": false,
  "tf_three_ema2": 21,
 
  "tf_three_useSupertrend": false,
  "tf_three_supertrend_length": 10,
  "tf_three_supertrend_factor": 3
}



*/
