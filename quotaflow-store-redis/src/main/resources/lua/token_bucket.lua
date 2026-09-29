-- ARGV: capacity, interval nanoseconds, target fingerprint, weight.
local p = parameters('tb', ARGV[1], ARGV[2], ARGV[3], ARGV[4], false)
local sec, nano = clock()
local state = decode(KEYS[1], p, sec, nano)
if p.weight > p.capacity then return {0, state.tokens, 0} end
local allowed = state.tokens >= p.weight
local delay = 0
if allowed then state.tokens = state.tokens - p.weight else delay = retry(p, state) end
write(KEYS[1], p, state, sec, nano)
return {allowed and 1 or 0, state.tokens, delay}
