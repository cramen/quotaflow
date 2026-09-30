-- Namespace-level membership administration/enrollment. No quota keys are touched.
local MAX = 9007199254740991
local function bad(reason) error('QF_RECOVERY_METADATA ' .. reason, 0) end
local function count(value)
    if not value or not string.match(value, '^%d+$') then bad('invalid counter') end
    local n = tonumber(value)
    if n > MAX then bad('counter exhausted') end
    return n
end
local function number(n) return string.format('%.0f', n) end
local key = KEYS[1]
if redis.call('TYPE', key).ok ~= 'hash' or redis.call('HGET', key, 'ready') ~= '1'
    or redis.call('HGET', key, 'version') ~= '2' then bad('namespace is not ready') end
local op = ARGV[1]
if op == 'provision' then
    local incarnation, digest, size = ARGV[2], ARGV[3], count(ARGV[4])
    if size < 1 or #ARGV ~= size + 4 then bad('invalid cohort shape') end
    if redis.call('HEXISTS', key, 'rc:incarnation') == 1 then bad('cohort already provisioned') end
    redis.call('HSET', key, 'rc:version', '1', 'rc:incarnation', incarnation, 'rc:digest', digest,
        'rc:size', number(size), 'rc:state', 'active', 'rc:sequence', '0', 'rc:base', incarnation)
    for i = 1, size do
        redis.call('HSET', key, 'rc:member:' .. i, ARGV[i+4], 'rc:generation:' .. i, '0')
    end
    return {1, incarnation, digest, size}
end
-- Explicit maintenance requires the caller to prove that every old writer stopped and debt drained.
-- A stable operation token makes a partially completed cross-slot transition retryable.
if op == 'maintenance_prepare' then
    local oldInc, token, nextDigest = ARGV[2], ARGV[3], ARGV[4]
    local nextSize, domainCount = count(ARGV[5]), count(ARGV[6])
    if nextSize < 1 or #ARGV ~= 6 + nextSize + domainCount then bad('invalid maintenance shape') end
    local plan = redis.sha1hex(table.concat(ARGV, '|', 2))
    local state = redis.call('HGET', key, 'rc:state')
    if redis.call('HGET', key, 'rc:maintenance-token') == token then
        if redis.call('HGET', key, 'rc:maintenance-digest') ~= nextDigest
            or redis.call('HGET', key, 'rc:maintenance-old') ~= oldInc
            or redis.call('HGET', key, 'rc:maintenance-plan') ~= plan then bad('maintenance token reused') end
        return {1, redis.call('HGET', key, 'rc:maintenance-next'), state}
    end
    if state ~= 'active' or redis.call('HGET', key, 'rc:incarnation') ~= oldInc then bad('maintenance baseline changed') end
    local supplied = {}
    for i = 1, domainCount do
        local domain = ARGV[6 + nextSize + i]
        if supplied[domain] or redis.call('HGET', key, 'rc:domain:' .. domain) ~= 'ready' then bad('invalid maintenance domain') end
        supplied[domain] = true
    end
    for _, field in ipairs(redis.call('HKEYS', key)) do
        if string.sub(field, 1, 10) == 'rc:domain:' and not supplied[string.sub(field, 11)] then
            bad('maintenance must cover every provisioned domain')
        end
    end
    local sequence = count(redis.call('HGET', key, 'rc:sequence'))
    if sequence == MAX then bad('incarnation counter exhausted') end
    sequence = sequence + 1
    local nextInc = 'generation:' .. number(sequence) .. ':' .. redis.call('HGET', key, 'rc:base')
    redis.call('HSET', key, 'rc:state', 'maintenance', 'rc:sequence', number(sequence),
        'rc:maintenance-token', token, 'rc:maintenance-old', oldInc, 'rc:maintenance-next', nextInc,
        'rc:maintenance-digest', nextDigest, 'rc:maintenance-size', number(nextSize), 'rc:maintenance-plan', plan)
    for i = 1, nextSize do redis.call('HSET', key, 'rc:next-member:' .. i, ARGV[6 + i]) end
    return {1, nextInc, 'maintenance'}
end
if op == 'maintenance_commit' then
    if ARGV[2] ~= redis.call('HGET', key, 'rc:maintenance-token') then bad('maintenance token mismatch') end
    if redis.call('HGET', key, 'rc:state') == 'active' then return {1} end
    if redis.call('HGET', key, 'rc:state') ~= 'maintenance' then bad('maintenance is not current') end
    local oldSize = count(redis.call('HGET', key, 'rc:size'))
    local nextSize = count(redis.call('HGET', key, 'rc:maintenance-size'))
    for i = 1, oldSize do redis.call('HDEL', key, 'rc:member:' .. i, 'rc:owner:' .. i, 'rc:generation:' .. i) end
    for i = 1, nextSize do
        redis.call('HSET', key, 'rc:member:' .. i, redis.call('HGET', key, 'rc:next-member:' .. i), 'rc:generation:' .. i, '0')
        redis.call('HDEL', key, 'rc:next-member:' .. i)
    end
    redis.call('HSET', key, 'rc:incarnation', redis.call('HGET', key, 'rc:maintenance-next'),
        'rc:digest', redis.call('HGET', key, 'rc:maintenance-digest'), 'rc:size', number(nextSize), 'rc:state', 'active')
    return {1}
end
if redis.call('HGET', key, 'rc:version') ~= '1' or redis.call('HGET', key, 'rc:state') ~= 'active' then
    bad('cohort is absent or undergoing maintenance')
end
if op == 'describe' then
    return {1, redis.call('HGET', key, 'rc:incarnation'), redis.call('HGET', key, 'rc:digest'),
        count(redis.call('HGET', key, 'rc:size'))}
end
if op == 'domain_prepare' or op == 'domain_commit' then
    if ARGV[2] ~= redis.call('HGET', key, 'rc:digest') or ARGV[3] ~= redis.call('HGET', key, 'rc:incarnation') then
        bad('domain cohort mismatch')
    end
    local field = 'rc:domain:' .. ARGV[4]
    local previous = redis.call('HGET', key, field)
    if op == 'domain_prepare' then
        local bound = false
        for _, registered in ipairs(redis.call('HKEYS', key)) do
            if string.sub(registered, 1, 2) == 'p:' then
                local root = string.match(redis.call('HGET', key, registered), '^[^:]+:([0-9a-f]+):[a-z]+$')
                if root == ARGV[4] then bound = true; break end
            end
        end
        if not bound then bad('domain has no registered policies') end
        if previous and previous ~= 'preparing' then bad('domain already registered; lost controllers require explicit maintenance') end
        redis.call('HSET', key, field, 'preparing')
    else
        if previous ~= 'preparing' then bad('domain preparation is not current') end
        redis.call('HSET', key, field, 'ready')
    end
    return {1}
end
local digest, slot, member, token = ARGV[2], count(ARGV[3]) + 1, ARGV[4], ARGV[5]
local size = count(redis.call('HGET', key, 'rc:size'))
if digest ~= redis.call('HGET', key, 'rc:digest') or slot > size
    or member ~= redis.call('HGET', key, 'rc:member:' .. slot) then bad('cohort declaration mismatch') end
local owner = redis.call('HGET', key, 'rc:owner:' .. slot)
local generation = count(redis.call('HGET', key, 'rc:generation:' .. slot))
if op == 'enroll' then
    if owner and owner ~= token then error('QF_RECOVERY_OWNERSHIP slot already owned', 0) end
    if not owner then
        if generation ~= 0 then bad('lost owner metadata') end
        generation = 1
        redis.call('HSET', key, 'rc:owner:' .. slot, token, 'rc:generation:' .. slot, '1')
    end
elseif op == 'validate' then
    if ARGV[6] and redis.call('HGET', key, 'rc:domain:' .. ARGV[6]) ~= 'ready' then bad('domain is not provisioned') end
    if not owner or owner ~= token then error('QF_RECOVERY_OWNERSHIP unproven owner', 0) end
else bad('unknown membership operation') end
return {1, redis.call('HGET', key, 'rc:incarnation'), digest, generation}
