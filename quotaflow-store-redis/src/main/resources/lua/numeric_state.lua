-- Shared exact credit arithmetic. All products stay below 2^53.
-- This prefix is loaded into every acquisition/seed script before execution.
local MAX_TOKENS = 1000000000
local HORIZON = 2764800000000000
local SECOND = 1000000000
local function invalid(reason) error('QF_STATE ' .. reason, 0) end
local function integer(value, maximum)
    if type(value) ~= 'string' or not string.match(value, '^%d+$') then invalid('invalid integer field') end
    local n = tonumber(value)
    if not n or n > maximum or n < 0 or n ~= math.floor(n) then invalid('integer outside codec bounds') end
    return n
end
local function quotient(n, d)
    local q = math.floor(n / d)
    if q * d > n then q = q - 1 end
    if (q + 1) * d <= n then q = q + 1 end
    return q
end
local function ceildiv(n, d)
    local q = quotient(n, d)
    return q + (n - q * d > 0 and 1 or 0)
end
local function fingerprint(value)
    if not value or #value ~= 64 or not string.match(value, '^[0-9a-f]+$') then invalid('invalid parameter fingerprint') end
    return value
end
local function parameters(algo, capacity, interval, digest, weight, seed)
    if algo ~= 'tb' and algo ~= 'gcra' then invalid('unknown algorithm') end
    local c = integer(capacity, MAX_TOKENS)
    local i = integer(interval, HORIZON)
    local w = integer(weight, MAX_TOKENS)
    if c < 1 or i < 1000 or c > quotient(HORIZON, i) or (not seed and w < 1) then
        invalid('unsupported numeric parameters')
    end
    if seed and w > c then invalid('seed balance exceeds target capacity') end
    return {algorithm=algo, capacity=c, interval=i, fingerprint=fingerprint(digest), weight=w}
end
local function clock()
    local t = redis.call('TIME')
    return tonumber(t[1]), tonumber(t[2]) * 1000
end
local function elapsed(sec, nano, oldsec, oldnano)
    if sec < oldsec or (sec == oldsec and nano < oldnano) then return 0, true end
    local seconds = sec - oldsec
    if seconds > 2764800 then return HORIZON, false end
    return math.min(HORIZON, seconds * SECOND + nano - oldnano), false
end
local function decode(key, p, sec, nano)
    local kind = redis.call('TYPE', key).ok
    if kind == 'none' then
        return {tokens=p.capacity, remainder=0, sec=sec, nano=nano, changed=false}
    end
    if kind ~= 'string' then invalid('bucket has incompatible Redis type') end
    local data = redis.call('GET', key)
    local version, algorithm, digest, tokens, remainder, seconds, nanos =
        string.match(data, '^(%d+):([a-z]+):([0-9a-f]+):(%d+):(%d+):(%d+):(%d+)$')
    if version ~= '3' or algorithm ~= p.algorithm then invalid('codec version or algorithm mismatch') end
    fingerprint(digest)
    local state = {tokens=integer(tokens, MAX_TOKENS), remainder=integer(remainder, HORIZON - 1),
        sec=integer(seconds, 9007199254740991), nano=integer(nanos, SECOND - 1), changed=digest ~= p.fingerprint}
    local progress, backward = elapsed(sec, nano, state.sec, state.nano)
    if backward then
        local future = state.sec - sec
        if future > 2764800 then invalid('timestamp exceeds supported backward horizon') end
    end
    if state.changed then
        state.tokens = math.min(state.tokens, p.capacity)
        state.remainder = 0
    else
        if state.tokens > p.capacity or state.remainder >= p.interval
            or (state.tokens == p.capacity and state.remainder ~= 0) then invalid('invalid balance for fingerprint') end
        local credit = state.remainder + progress
        local whole = quotient(credit, p.interval)
        state.tokens = math.min(p.capacity, state.tokens + whole)
        state.remainder = state.tokens == p.capacity and 0 or credit - whole * p.interval
    end
    if not backward then state.sec = sec; state.nano = nano end
    return state
end
local function write(key, p, state, sec, nano)
    local data = string.format('3:%s:%s:%.0f:%.0f:%.0f:%.0f', p.algorithm, p.fingerprint,
        state.tokens, state.remainder, state.sec, state.nano)
    local debt = (p.capacity - state.tokens) * p.interval - state.remainder
    -- Retain future accounted time after a backward clock observation.
    local future = math.max(0, (state.sec - sec) * SECOND + state.nano - nano)
    local ttl = math.max(1, ceildiv(debt + future, 1000000))
    redis.call('SET', key, data, 'PX', string.format('%.0f', ttl))
end
local function retry(p, state)
    return ceildiv((p.weight - state.tokens) * p.interval - state.remainder, 1000000)
end
