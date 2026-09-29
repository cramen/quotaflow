-- ARGV per key: algorithm, target capacity, interval, target fingerprint, whole balance.
-- Recovery generation authorization is supplied by the subsequent recovery protocol.
if #KEYS == 0 or #ARGV ~= #KEYS * 5 then invalid('invalid seed shape') end
local sec, nano = clock()
local ps, states = {}, {}
for i = 1, #KEYS do
    local b = (i - 1) * 5
    ps[i] = parameters(ARGV[b+1], ARGV[b+2], ARGV[b+3], ARGV[b+4], ARGV[b+5], true)
    states[i] = decode(KEYS[i], ps[i], sec, nano)
    states[i].tokens = math.min(states[i].tokens, ps[i].weight)
    states[i].remainder = 0
end
for i = 1, #KEYS do write(KEYS[i], ps[i], states[i], sec, nano) end
return {1}
