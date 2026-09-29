-- Atomic token bucket acquisition. Every timestamp comes from the storage
-- server (TIME); client clocks never participate. On every call the bucket is
-- refilled from the last stored timestamp, then the weight is consumed if the
-- bucket holds enough tokens; nothing is consumed otherwise. The key carries
-- a PEXPIRE equal to the time the bucket needs to refill to full, so idle
-- state self-evicts.
--
-- KEYS[1]: bucket key
-- ARGV[1]: capacity (tokens)
-- ARGV[2]: refillAmount (tokens per period)
-- ARGV[3]: refillPeriod (microseconds)
-- ARGV[4]: weight (tokens requested)
--
-- State format: "<tokens>:<sec>:<usec>" — seconds and microseconds are stored
-- separately so no large timestamp ever crosses Lua's lossy %.14g number
-- formatting.
--
-- Returns: {acquired (1/0), remaining (whole tokens), retryAfterMillis (0 on allow)}

local t = redis.call('TIME')
local now = t[1] * 1000000 + t[2]

local capacity = tonumber(ARGV[1])
local interval = tonumber(ARGV[3]) / tonumber(ARGV[2])
local weight = tonumber(ARGV[4])

local tokens = capacity
local data = redis.call('GET', KEYS[1])
if data then
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

local acquired = 0
local retry_after = 0
if tokens >= weight then
  acquired = 1
  tokens = tokens - weight
else
  retry_after = math.max(1, math.ceil((weight - tokens) * interval / 1000))
end

redis.call('SET', KEYS[1], string.format('%.14g:%d:%d', tokens, t[1], t[2]))
local ttl_millis = math.max(1, math.ceil((capacity - tokens) * interval / 1000))
redis.call('PEXPIRE', KEYS[1], ttl_millis)

return {acquired, math.floor(tokens), retry_after}
