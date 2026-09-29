-- Explicit offline provisioning only; acquisition never calls this script.
if redis.call('EXISTS', KEYS[1]) == 1 then return {0} end
local maximum = tonumber(ARGV[1])
if not maximum or maximum < 1 or maximum ~= math.floor(maximum) then return {-1} end
local now = redis.call('TIME')
redis.call('HSET', KEYS[1], 'version', '2', 'ready', '1', 'maxPolicies', maximum,
    'count', 0, 'mode', ARGV[2], 'createdAt', now[1])
return {1}
