-- ARGV per key: algorithm, capacity, interval nanoseconds, fingerprint, weight.
if #KEYS == 0 or #ARGV ~= #KEYS * 5 then invalid('invalid chain shape') end
local sec, nano = clock()
local ps, states = {}, {}
local failed, impossible = nil, nil
for i = 1, #KEYS do
    local b = (i - 1) * 5
    local p = parameters(ARGV[b+1], ARGV[b+2], ARGV[b+3], ARGV[b+4], ARGV[b+5], false)
    ps[i] = p
    states[i] = decode(KEYS[i], p, sec, nano)
    if p.weight > p.capacity and not impossible then impossible = i end
    if states[i].tokens < p.weight and not failed then failed = i end
end
-- Each pair is the capacity and remaining amount from this same evaluation.
local function observed(result)
    for i = 1, #KEYS do
        result[#result + 1] = ps[i].capacity
        result[#result + 1] = states[i].tokens
    end
    return result
end
-- Nothing is written before the entire batch has been decoded and validated.
if impossible then return observed({0, impossible - 1, states[impossible].tokens, 0}) end
if failed then
    -- Configuration normalization is durable even when a request cannot acquire.
    for i = 1, #KEYS do
        if states[i].changed then write(KEYS[i], ps[i], states[i], sec, nano) end
    end
    return observed({0, failed - 1, states[failed].tokens, retry(ps[failed], states[failed])})
end
local remaining = MAX_TOKENS
for i = 1, #KEYS do
    states[i].tokens = states[i].tokens - ps[i].weight
    remaining = math.min(remaining, states[i].tokens)
    write(KEYS[i], ps[i], states[i], sec, nano)
end
return observed({1, #KEYS - 1, remaining, 0})
