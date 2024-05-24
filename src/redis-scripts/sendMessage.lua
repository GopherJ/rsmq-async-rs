local ns = KEYS[1]
local qname = KEYS[2]
local ts_delay = tonumber(KEYS[3])
local queue_uid = KEYS[4]
local message = KEYS[5]
local realtime = tonumber(KEYS[6])

redis.call('ZADD', ns .. qname, ts_delay, queue_uid)

redis.call('HSET', ns .. qname .. ':Q', queue_uid, message)

local result = redis.call('HINCRBY', ns .. qname .. ':Q', 'totalsent', 1)

if realtime == 1 then
    result = redis.call('ZCARD', ns .. qname)
    redis.call('PUBLISH', ns .. ':rt:' .. qname, result)
end

return result
