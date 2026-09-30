-- Operation context is checked in the same execution/slot as quota mutation.
local function recovery_authorize(kind)
    local function bad(reason) error('QF_RECOVERY_METADATA ' .. reason, 0) end
    local function integer(v)
        if not v or not string.match(v, '^%d+$') then bad('invalid operation counter') end
        local n = tonumber(v)
        if n > 9007199254740991 then bad('operation counter exhausted') end
        return n
    end
    if #ARGV < 14 or #KEYS < 1 or (kind == 'acquire' and #KEYS < 2) or ARGV[1] ~= 'qf-recovery-v1' then bad('missing operation context') end
    local key = KEYS[1]
    if redis.call('TYPE', key).ok ~= 'hash' or redis.call('HGET', key, 'version') ~= '1'
        or redis.call('PTTL', key) ~= -1 then bad('controller is absent or incompatible') end
    if ARGV[2] ~= redis.call('HGET', key, 'incarnation') or ARGV[3] ~= redis.call('HGET', key, 'digest') then
        bad('cohort context mismatch')
    end
    local slot = integer(ARGV[4])
    local size = integer(redis.call('HGET', key, 'size'))
    if slot >= size then bad('unknown cohort slot') end
    if integer(ARGV[5]) ~= integer(redis.call('HGET', key, 'session:' .. slot))
        or ARGV[6] ~= redis.call('HGET', key, 'owner:' .. slot) then
        error('QF_RECOVERY_OWNERSHIP stale operation session', 0)
    end
    local epoch = integer(redis.call('HGET', key, 'epoch'))
    local dispatch = integer(redis.call('HGET', key, 'dispatch'))
    local configuration = integer(redis.call('HGET', key, 'configuration'))
    local retired = integer(redis.call('HGET', key, 'retired'))
    local resolver = integer(redis.call('HGET', key, 'resolver'))
    local resolverFp = redis.call('HGET', key, 'resolver-fingerprint')
    local resolverFloor = integer(redis.call('HGET', key, 'resolver-floor'))
    local resolverFloorFp = redis.call('HGET', key, 'resolver-floor-fingerprint')
    local none = string.rep('0', 64)
    local function validResolver(revision, fingerprint)
        return fingerprint and #fingerprint == 64 and string.match(fingerprint, '^[0-9a-f]+$')
            and ((revision == 0 and fingerprint == none) or (revision > 0 and fingerprint ~= none))
    end
    if not validResolver(resolver, resolverFp) or not validResolver(resolverFloor, resolverFloorFp)
        or (resolver > 0 and (resolver ~= resolverFloor or resolverFp ~= resolverFloorFp)) then bad('invalid resolver ordering metadata') end
    local phase = redis.call('HGET', key, 'phase')
    local fp = redis.call('HGET', key, 'fingerprint')
    if not fp or #fp ~= 64 or not string.match(fp, '^[0-9a-f]+$') then bad('invalid configuration fingerprint') end
    local joined = integer(redis.call('HGET', key, 'joined'))
    local ready = integer(redis.call('HGET', key, 'ready'))
    if size < 1 or retired > epoch or (phase == 'NORMAL' and (joined ~= size or ready ~= size or retired ~= epoch))
        or (phase == 'GATHER' and (joined >= size or ready ~= 0 or retired >= epoch))
        or (phase == 'DRAIN' and (joined ~= size or ready >= size or retired >= epoch))
        or (phase ~= 'NORMAL' and phase ~= 'GATHER' and phase ~= 'DRAIN') then bad('invalid controller phase') end
    if integer(ARGV[7]) ~= epoch or integer(ARGV[8]) ~= dispatch
        or integer(ARGV[9]) ~= configuration or ARGV[10] ~= fp or ARGV[11] ~= phase
        or integer(ARGV[13]) ~= resolver or ARGV[14] ~= resolverFp then return false end
    if kind == 'acquire' then
        if phase == 'DRAIN' then return false end
        if phase == 'GATHER' then
            if ARGV[12] ~= '1' then return false end
            local stamp = string.format('%.0f:%.0f:%.0f:%.0f', epoch, dispatch, configuration, integer(ARGV[5]))
            if redis.call('HGET', key, 'join:' .. slot) ~= stamp then return false end
        end
    elseif kind == 'seed' then
        if phase == 'NORMAL' then return false end
    else bad('invalid operation kind') end
    for i = 1, 14 do table.remove(ARGV, 1) end
    table.remove(KEYS, 1)
    return true
end
