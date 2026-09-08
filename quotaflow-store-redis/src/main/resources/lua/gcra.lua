-- Atomic GCRA (generic cell rate algorithm) acquisition. Every timestamp
-- comes from the storage server (TIME); client clocks never participate.
-- The stored theoretical arrival time (TAT) is advanced by weight * emission
-- interval if the request stays within tolerance; nothing is stored otherwise.
-- The key carries a PEXPIRE equal to the time the TAT needs to drain back to
-- the present, so idle state self-evicts.
--
-- KEYS[1]: TAT key
-- ARGV[1]: capacity (burst size in tokens)
-- ARGV[2]: refillAmount (tokens per period)
-- ARGV[3]: refillPeriod (microseconds)
-- ARGV[4]: weight (tokens requested)
--
-- State format: "<sec>:<usec>" — the TAT stored as separate seconds and
-- microseconds so no large timestamp crosses Lua's lossy %.14g formatting.
--
-- Returns: {acquired (1/0), remaining (whole tokens), retryAfterMillis (0 on allow)}

local t = redis.call('TIME')
local now = t[1] * 1000000 + t[2]

local capacity = tonumber(ARGV[1])
local interval = tonumber(ARGV[3]) / tonumber(ARGV[2])
local weight = tonumber(ARGV[4])
local tau = capacity * interval

local tat = now
local exists = false
local data = redis.call('GET', KEYS[1])
if data then
  exists = true
  local sep = string.find(data, ':')
  local stored = tonumber(string.sub(data, 1, sep - 1)) * 1000000
      + tonumber(string.sub(data, sep + 1))
  if stored > now then tat = stored end
end

local candidate = tat + weight * interval
local overdraft = candidate - now

local acquired = 0
local remaining
local retry_after = 0
local new_tat
if overdraft <= tau then
  acquired = 1
  new_tat = candidate
  remaining = math.floor((tau - overdraft) / interval)
else
  new_tat = tat
  remaining = math.floor((tau - (tat - now)) / interval)
  retry_after = math.max(1, math.ceil((overdraft - tau) / 1000))
end
if remaining < 0 then remaining = 0 end
if remaining > capacity then remaining = capacity end

local drain = new_tat - now
if drain <= 0 then
  -- A TAT at or behind the present carries no information; drop the key.
  if exists then redis.call('DEL', KEYS[1]) end
else
  redis.call('SET', KEYS[1], string.format('%d:%d', math.floor(new_tat / 1000000), math.floor(new_tat % 1000000)))
  redis.call('PEXPIRE', KEYS[1], math.max(1, math.ceil(drain / 1000)))
end

return {acquired, remaining, retry_after}
