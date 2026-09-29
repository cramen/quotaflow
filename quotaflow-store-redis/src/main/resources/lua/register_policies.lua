-- Atomic namespace readiness and immutable policy-binding registration.
-- One manifest key; all validation precedes the first mutation.
-- ARGV: repeated policy digest, scope/root binding pairs.
local kind = redis.call('TYPE', KEYS[1]).ok
if kind == 'none' then return {-1} end
if kind ~= 'hash' then return {-4} end
if redis.call('HGET', KEYS[1], 'version') ~= '2'
    or redis.call('HGET', KEYS[1], 'ready') ~= '1' then return {-1} end
local maximum = tonumber(redis.call('HGET', KEYS[1], 'maxPolicies'))
local count = tonumber(redis.call('HGET', KEYS[1], 'count'))
if not maximum or maximum < 1 or maximum > 2147483647 or maximum ~= math.floor(maximum)
    or not count or count < 0 or count ~= math.floor(count) or count > maximum then return {-4} end
local actual = 0
for _, field in ipairs(redis.call('HKEYS', KEYS[1])) do
    if string.sub(field, 1, 2) == 'p:' then
        local value = redis.call('HGET', KEYS[1], field)
        local split = string.find(value, ':')
        local scope = split and string.sub(value, 1, split - 1)
        local root, algorithm = string.match(value, '^[^:]+:([0-9a-f]+):([a-z]+)$')
        if #field ~= 66 or not string.match(string.sub(field, 3), '^[0-9a-f]+$')
            or (algorithm ~= 'tb' and algorithm ~= 'gcra')
            or not root or #root ~= 64 or not string.match(root, '^[0-9a-f]+$')
            or (scope ~= 'global' and scope ~= 'tenant' and scope ~= 'user' and scope ~= 'key') then return {-4} end
        actual = actual + 1
    end
end
if actual ~= count then return {-4} end
local pending = {}
local added = 0
for i = 1, #ARGV, 2 do
    local field = 'p:' .. ARGV[i]
    local wanted = ARGV[i + 1]
    local old = redis.call('HGET', KEYS[1], field)
    if (old and old ~= wanted) or (pending[field] and pending[field] ~= wanted) then return {-2} end
    if not old and not pending[field] then
        pending[field] = wanted
        added = added + 1
    end
end
if count + added > maximum then return {-3} end
for field, value in pairs(pending) do redis.call('HSET', KEYS[1], field, value) end
redis.call('HSET', KEYS[1], 'count', count + added)
return {1}
