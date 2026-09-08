-- Atomic policy-chain evaluation: the whole chain in one execution.
-- Pass 1 refills every level from server time (TIME) and checks sufficiency
-- at every level. If all levels admit the request, pass 2 deducts at every
-- level; otherwise nothing is written anywhere — a rejection at any level
-- consumes no tokens at any level.
--
-- KEYS[1..n]: one key per level, ordered root-to-leaf. All keys of one chain
-- share a hash tag, so this script is a legal same-slot multi-key script in
-- Redis Cluster.
-- ARGV: per level, five values —
--   ARGV[(i-1)*5+1]: algorithm ('tb' or 'gcra')
--   ARGV[(i-1)*5+2]: capacity
--   ARGV[(i-1)*5+3]: refillAmount
--   ARGV[(i-1)*5+4]: refillPeriod (microseconds)
--   ARGV[(i-1)*5+5]: weight (tokens requested at this level)
--
-- State formats match the single-key scripts:
--   token bucket: "<tokens>:<sec>:<usec>", GCRA: "<sec>:<usec>" (TAT).
--
-- Returns: {acquired (1/0), firedLevelIndex (0-based; leaf on allow),
--           remaining (fired level on reject, chain minimum on allow),
--           retryAfterMillis (0 on allow)}

local t = redis.call('TIME')
local now = t[1] * 1000000 + t[2]

local n = #KEYS

local levels = {}
for i = 1, n do
  local base = (i - 1) * 5
  local algo = ARGV[base + 1]
  local capacity = tonumber(ARGV[base + 2])
  local interval = tonumber(ARGV[base + 4]) / tonumber(ARGV[base + 3])
  local weight = tonumber(ARGV[base + 5])
  local level = {algo = algo, capacity = capacity, interval = interval,
                 exists = false, sufficient = false}
  local data = redis.call('GET', KEYS[i])
  if algo == 'tb' then
    local tokens = capacity
    if data then
      level.exists = true
      local first = string.find(data, ':')
      local second = string.find(data, ':', first + 1)
      local stored = tonumber(string.sub(data, 1, first - 1))
      local last = tonumber(string.sub(data, first + 1, second - 1)) * 1000000
          + tonumber(string.sub(data, second + 1))
      local elapsed = now - last
      if elapsed < 0 then elapsed = 0 end
      tokens = stored + elapsed / interval
      if tokens > capacity then tokens = capacity end
    end
    if tokens >= weight then
      level.sufficient = true
      level.tokens = tokens - weight
      level.remaining = math.floor(level.tokens)
    else
      level.tokens = tokens
      level.remaining = math.floor(tokens)
      level.retry = math.max(1, math.ceil((weight - tokens) * interval / 1000))
    end
  else
    local tau = capacity * interval
    local tat = now
    if data then
      level.exists = true
      local sep = string.find(data, ':')
      local stored = tonumber(string.sub(data, 1, sep - 1)) * 1000000
          + tonumber(string.sub(data, sep + 1))
      if stored > now then tat = stored end
    end
    local overdraft = tat + weight * interval - now
    if overdraft <= tau then
      level.sufficient = true
      level.tat = tat + weight * interval
      level.remaining = math.floor((tau - overdraft) / interval)
    else
      level.tat = tat
      level.remaining = math.floor((tau - (tat - now)) / interval)
      level.retry = math.max(1, math.ceil((overdraft - tau) / 1000))
    end
    if level.remaining < 0 then level.remaining = 0 end
    if level.remaining > capacity then level.remaining = capacity end
  end
  levels[i] = level
end

local fired = 0
for i = 1, n do
  if not levels[i].sufficient then
    fired = i
    break
  end
end

if fired > 0 then
  local level = levels[fired]
  return {0, fired - 1, level.remaining, level.retry}
end

local min_remaining = levels[1].remaining
for i = 1, n do
  local level = levels[i]
  if level.remaining < min_remaining then min_remaining = level.remaining end
  if level.algo == 'tb' then
    redis.call('SET', KEYS[i],
        string.format('%.14g:%d:%d', level.tokens, t[1], t[2]))
    local ttl = math.max(1, math.ceil((level.capacity - level.tokens) * level.interval / 1000))
    redis.call('PEXPIRE', KEYS[i], ttl)
  else
    local drain = level.tat - now
    if drain <= 0 then
      if level.exists then redis.call('DEL', KEYS[i]) end
    else
      redis.call('SET', KEYS[i],
          string.format('%d:%d', math.floor(level.tat / 1000000), math.floor(level.tat % 1000000)))
      redis.call('PEXPIRE', KEYS[i], math.max(1, math.ceil(drain / 1000)))
    end
  end
end

return {1, n - 1, min_remaining, 0}
