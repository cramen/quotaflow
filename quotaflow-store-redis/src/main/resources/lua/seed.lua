-- Best-effort recovery seeding: writes locally measured remaining capacity
-- into the distributed state for one key, merging conservatively — the stored
-- remaining after seeding is never higher than what the store already holds
-- (min of the refilled stored state and the local snapshot), so consumption
-- measured by either side during the outage is preserved.
--
-- KEYS[1]: bucket key (already mapped by the caller's key scheme)
-- ARGV[1]: algorithm ('tb' or 'gcra')
-- ARGV[2]: capacity
-- ARGV[3]: refillAmount
-- ARGV[4]: refillPeriod (microseconds)
-- ARGV[5]: local remaining (whole tokens at snapshot time)
--
-- State formats match the acquisition scripts:
--   token bucket: "<tokens>:<sec>:<usec>", GCRA: "<sec>:<usec>" (TAT).
--
-- Returns: {1, merged remaining}

local t = redis.call('TIME')
local now = t[1] * 1000000 + t[2]

local algo = ARGV[1]
local capacity = tonumber(ARGV[2])
local interval = tonumber(ARGV[4]) / tonumber(ARGV[3])
local local_remaining = tonumber(ARGV[5])
if local_remaining < 0 then local_remaining = 0 end
if local_remaining > capacity then local_remaining = capacity end

local data = redis.call('GET', KEYS[1])

if algo == 'tb' then
  local tokens = capacity
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
  local merged = math.min(math.floor(tokens), local_remaining)
  redis.call('SET', KEYS[1], string.format('%.14g:%d:%d', merged, t[1], t[2]))
  local ttl = math.max(1, math.ceil((capacity - merged) * interval / 1000))
  redis.call('PEXPIRE', KEYS[1], ttl)
  return {1, merged}
else
  local tau = capacity * interval
  local tat = now
  local exists = false
  if data then
    exists = true
    local sep = string.find(data, ':')
    local stored = tonumber(string.sub(data, 1, sep - 1)) * 1000000
        + tonumber(string.sub(data, sep + 1))
    if stored > now then tat = stored end
  end
  local current_remaining = math.floor((tau - (tat - now)) / interval)
  if current_remaining < 0 then current_remaining = 0 end
  if current_remaining > capacity then current_remaining = capacity end
  -- smaller remaining means a later TAT: min on remaining is max on TAT
  local merged = math.min(current_remaining, local_remaining)
  local new_tat = now + (tau - merged * interval)
  local drain = new_tat - now
  if drain <= 0 then
    if exists then redis.call('DEL', KEYS[1]) end
  else
    redis.call('SET', KEYS[1], string.format('%d:%d', math.floor(new_tat / 1000000), math.floor(new_tat % 1000000)))
    redis.call('PEXPIRE', KEYS[1], math.max(1, math.ceil(drain / 1000)))
  end
  return {1, merged}
end
