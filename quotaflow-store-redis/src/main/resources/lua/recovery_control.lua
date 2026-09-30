-- Per-domain control transactions. KEYS[1] is the reserved same-slot control key.
-- Every record is fixed-size per declared member; readiness never expires by timeout.
local MAX = 9007199254740991
local NONE = string.rep('0', 64)
local function bad(reason) error('QF_RECOVERY_METADATA ' .. reason, 0) end
local function integer(v)
    if not v or not string.match(v, '^%d+$') then bad('invalid control counter') end
    local n = tonumber(v)
    if n > MAX then bad('control counter overflow') end
    return n
end
local function decimal(n) return string.format('%.0f', n) end
local function advance(n) if n == MAX then bad('control counter exhausted') end; return n + 1 end
local key = KEYS[1]
local op = ARGV[1]
if op == 'provision' then
    local size = integer(ARGV[4])
    if size < 1 or #ARGV ~= 5 + size * 3 then bad('invalid provisioning shape') end
    if redis.call('EXISTS', key) ~= 0 then
        if redis.call('TYPE', key).ok ~= 'hash' or redis.call('PTTL', key) ~= -1
            or redis.call('HGET', key, 'version') ~= '1'
            or redis.call('HGET', key, 'resolver') ~= '0' or redis.call('HGET', key, 'resolver-fingerprint') ~= NONE
            or redis.call('HGET', key, 'resolver-floor') ~= '0' or redis.call('HGET', key, 'resolver-floor-fingerprint') ~= NONE
            or redis.call('HGET', key, 'incarnation') ~= ARGV[2] or redis.call('HGET', key, 'digest') ~= ARGV[3]
            or redis.call('HGET', key, 'fingerprint') ~= ARGV[5] or redis.call('HGET', key, 'size') ~= decimal(size)
            or redis.call('HGET', key, 'epoch') ~= '1' or redis.call('HGET', key, 'dispatch') ~= '1'
            or redis.call('HGET', key, 'configuration') ~= '0' or redis.call('HGET', key, 'phase') ~= 'GATHER'
            or redis.call('HGET', key, 'retired') ~= '0' or redis.call('HGET', key, 'joined') ~= '0'
            or redis.call('HGET', key, 'ready') ~= '0' then bad('controller is not an unused preparation') end
        for slot = 0, size - 1 do
            if redis.call('HGET', key, 'member:' .. slot) ~= ARGV[6 + slot * 3]
                or redis.call('HGET', key, 'session:' .. slot) ~= '0'
                or redis.call('HGET', key, 'owner:' .. slot) ~= '' then bad('controller preparation has an owner') end
        end
        return {1}
    end
    redis.call('HSET', key, 'version', '1', 'incarnation', ARGV[2], 'digest', ARGV[3],
        'size', decimal(size), 'epoch', '1', 'dispatch', '1', 'retired', '0', 'configuration', '0', 'fingerprint', ARGV[5],
        'phase', 'GATHER', 'joined', '0', 'ready', '0', 'resolver', '0', 'resolver-fingerprint', NONE,
        'resolver-floor', '0', 'resolver-floor-fingerprint', NONE)
    for slot = 0, size - 1 do
        local b = 6 + slot * 3
        redis.call('HSET', key, 'member:' .. slot, ARGV[b], 'session:' .. slot, ARGV[b+1],
            'owner:' .. slot, ARGV[b+2], 'local-config:' .. slot, '0')
    end
    return {1}
end
if redis.call('TYPE', key).ok ~= 'hash' or redis.call('HGET', key, 'version') ~= '1'
    or redis.call('PTTL', key) ~= -1 then bad('controller is absent, corrupt or expiring') end
if op == 'maintenance' then
    local oldInc, nextInc, nextDigest, token = ARGV[2], ARGV[3], ARGV[4], ARGV[5]
    local nextSize = integer(ARGV[6])
    if nextSize < 1 or #ARGV ~= 6 + nextSize then bad('invalid maintenance controller shape') end
    if redis.call('HGET', key, 'maintenance-token') == token then
        if redis.call('HGET', key, 'incarnation') ~= nextInc or redis.call('HGET', key, 'digest') ~= nextDigest then bad('maintenance token reused') end
        return {1}
    end
    if redis.call('HGET', key, 'incarnation') ~= oldInc then bad('maintenance controller baseline changed') end
    local epoch = integer(redis.call('HGET', key, 'epoch'))
    local dispatch = advance(integer(redis.call('HGET', key, 'dispatch')))
    local nextEpoch = advance(epoch)
    local size = integer(redis.call('HGET', key, 'size'))
    for slot = 0, size - 1 do
        redis.call('HDEL', key, 'member:' .. slot, 'session:' .. slot, 'owner:' .. slot,
            'local-config:' .. slot, 'local-fingerprint:' .. slot, 'join:' .. slot, 'ready:' .. slot)
    end
    for slot = 0, nextSize - 1 do
        redis.call('HSET', key, 'member:' .. slot, ARGV[7 + slot], 'session:' .. slot, '0',
            'owner:' .. slot, '', 'local-config:' .. slot, '0')
    end
    redis.call('HSET', key, 'incarnation', nextInc, 'digest', nextDigest, 'size', decimal(nextSize),
        'maintenance-token', token, 'epoch', decimal(nextEpoch), 'dispatch', decimal(dispatch),
        'retired', decimal(epoch), 'phase', 'GATHER', 'joined', '0', 'ready', '0')
    return {1}
end
local incarnation, digest = ARGV[2], ARGV[3]
local slot, session, token = integer(ARGV[4]), integer(ARGV[5]), ARGV[6]
local size = integer(redis.call('HGET', key, 'size'))
if incarnation ~= redis.call('HGET', key, 'incarnation') or digest ~= redis.call('HGET', key, 'digest')
    or slot >= size or ARGV[7] ~= redis.call('HGET', key, 'member:' .. slot) then bad('controller cohort mismatch') end
local storedSession = integer(redis.call('HGET', key, 'session:' .. slot))
local initialAttach = false
if op == 'attach' then
    -- The driver has validated enrollment against the namespace manifest first.
    -- An occupied domain slot cannot be replaced by a runtime attach.
    if storedSession == 0 and redis.call('HGET', key, 'owner:' .. slot) == '' then
        if session ~= 1 then bad('initial session generation is not one') end
        initialAttach = true
    end
end
if not initialAttach and (storedSession ~= session or token ~= redis.call('HGET', key, 'owner:' .. slot)) then
    error('QF_RECOVERY_OWNERSHIP stale or unknown session', 0)
end
local epoch = integer(redis.call('HGET', key, 'epoch'))
local dispatch = integer(redis.call('HGET', key, 'dispatch'))
local configuration = integer(redis.call('HGET', key, 'configuration'))
local retired = integer(redis.call('HGET', key, 'retired'))
local fp = redis.call('HGET', key, 'fingerprint')
local phase = redis.call('HGET', key, 'phase')
local joined = integer(redis.call('HGET', key, 'joined'))
local ready = integer(redis.call('HGET', key, 'ready'))
if (phase ~= 'NORMAL' and phase ~= 'GATHER' and phase ~= 'DRAIN') or joined > size or ready > size then
    bad('invalid phase or readiness counts')
end
if not fp or #fp ~= 64 or not string.match(fp, '^[0-9a-f]+$') then bad('invalid configuration fingerprint') end
if retired > epoch or (phase == 'NORMAL' and (joined ~= size or ready ~= size or retired ~= epoch))
    or (phase == 'GATHER' and (joined >= size or ready ~= 0 or retired >= epoch))
    or (phase == 'DRAIN' and (joined ~= size or ready >= size or retired >= epoch)) then bad('inconsistent phase counts') end
local resolver = integer(redis.call('HGET', key, 'resolver'))
local resolverFp = redis.call('HGET', key, 'resolver-fingerprint')
local resolverFloor = integer(redis.call('HGET', key, 'resolver-floor'))
local resolverFloorFp = redis.call('HGET', key, 'resolver-floor-fingerprint')
local function validResolver(revision, fingerprint)
    return fingerprint and #fingerprint == 64 and string.match(fingerprint, '^[0-9a-f]+$')
        and ((revision == 0 and fingerprint == NONE) or (revision > 0 and fingerprint ~= NONE))
end
if not validResolver(resolver, resolverFp) or not validResolver(resolverFloor, resolverFloorFp)
    or (resolver > 0 and (resolver ~= resolverFloor or resolverFp ~= resolverFloorFp)) then bad('invalid resolver ordering metadata') end
if initialAttach then redis.call('HSET', key, 'session:' .. slot, '1', 'owner:' .. slot, token) end
local function response(status)
    return {status, epoch, dispatch, configuration, fp, phase, joined, ready, retired, resolver, resolverFp, resolverFloor, resolverFloorFp}
end
local function publish()
    redis.call('HSET', key, 'epoch', decimal(epoch), 'dispatch', decimal(dispatch),
        'configuration', decimal(configuration), 'fingerprint', fp, 'phase', phase,
        'joined', decimal(joined), 'ready', decimal(ready), 'retired', decimal(retired),
        'resolver', decimal(resolver), 'resolver-fingerprint', resolverFp,
        'resolver-floor', decimal(resolverFloor), 'resolver-floor-fingerprint', resolverFloorFp)
end
if op == 'read' or op == 'attach' then return response(1) end
if op == 'begin' or op == 'configure' then
    if integer(ARGV[10]) ~= epoch or integer(ARGV[11]) ~= dispatch or integer(ARGV[12]) ~= configuration
        or ARGV[13] ~= fp or ARGV[14] ~= phase or integer(ARGV[17]) ~= resolver or ARGV[18] ~= resolverFp then return response(0) end
    local wanted, revision = ARGV[8], integer(ARGV[9])
    local proposedResolver, proposedFp = integer(ARGV[15]), ARGV[16]
    if not validResolver(proposedResolver, proposedFp) then bad('invalid proposed resolver identity') end
    if proposedResolver > 0 and proposedResolver < resolverFloor then return response(0) end
    if proposedResolver > 0 and proposedResolver == resolverFloor and proposedFp ~= resolverFloorFp then
        bad('resolver revision reused with different content')
    end
    if proposedResolver == 0 and resolver > 0 and wanted == fp then return response(0) end
    local prior = integer(redis.call('HGET', key, 'local-config:' .. slot))
    if revision < prior or (wanted ~= fp and revision <= prior) then return response(0) end
    if revision == prior then
        local priorFp = redis.call('HGET', key, 'local-fingerprint:' .. slot)
        if priorFp and priorFp ~= wanted then return response(0) end
    end
    -- Compute every counter increment before any mutation.
    local changed = wanted ~= fp or revision > prior or proposedResolver ~= resolver or proposedFp ~= resolverFp
    if op == 'configure' and phase == 'NORMAL' then
        if changed then dispatch = advance(dispatch); configuration = advance(configuration); fp = wanted end
    elseif phase == 'NORMAL' or changed then
        epoch = advance(epoch); dispatch = advance(dispatch)
        if changed then configuration = advance(configuration) end
        fp = wanted; phase = 'GATHER'; joined = 0; ready = 0
    end
    resolver, resolverFp = proposedResolver, proposedFp
    if resolver > resolverFloor then resolverFloor, resolverFloorFp = resolver, resolverFp end
    redis.call('HSET', key, 'local-config:' .. slot, decimal(revision), 'local-fingerprint:' .. slot, wanted)
    publish()
    return response(1)
end
if integer(ARGV[8]) ~= epoch or integer(ARGV[9]) ~= dispatch
    or integer(ARGV[10]) ~= configuration or ARGV[11] ~= fp
    or integer(ARGV[12]) ~= resolver or ARGV[13] ~= resolverFp then return response(0) end
local stamp = decimal(epoch) .. ':' .. decimal(dispatch) .. ':' .. decimal(configuration) .. ':' .. decimal(session)
if op == 'join' then
    if phase ~= 'GATHER' then return response(0) end
    if redis.call('HGET', key, 'join:' .. slot) ~= stamp then
        if joined + 1 == size then dispatch = advance(dispatch) end
        redis.call('HSET', key, 'join:' .. slot, stamp)
        joined = joined + 1
        if joined == size then phase = 'DRAIN'; ready = 0 end
    end
elseif op == 'ready' then
    if phase == 'NORMAL' and redis.call('HGET', key, 'ready:' .. slot) == stamp then return response(1) end
    if phase ~= 'DRAIN' then return response(0) end
    if redis.call('HGET', key, 'ready:' .. slot) ~= stamp then
        redis.call('HSET', key, 'ready:' .. slot, stamp)
        ready = ready + 1
        if ready == size then phase = 'NORMAL'; retired = epoch end
    end
elseif op == 'abort' then
    if phase ~= 'DRAIN' then return response(0) end
    dispatch = advance(dispatch); phase = 'GATHER'; joined = 0; ready = 0
else bad('unknown control operation') end
publish()
return response(1)
