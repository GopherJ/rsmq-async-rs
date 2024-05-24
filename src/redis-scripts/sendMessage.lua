local ns = KEYS[1]
local qname = KEYS[2]
local ts_delay = tonumber(KEYS[3])
local queue_uids = cjson.decode(KEYS[4])
local messages = cjson.decode(KEYS[5])
local realtime = tonumber(KEYS[6])

local key = ns .. qname
local queue_key = ns .. qname .. ':Q'
local realtime_key = ns .. ':rt:' .. qname

for i = 1, #queue_uids do

redis.call('ZADD', key, ts_delay, queue_uids[i])
redis.call('HSET', queue_key, queue_uids[i], messages[i])

end

local result = redis.call('HINCRBY', queue_key, 'totalsent', #queue_uids)

if realtime == 1 then
    result = redis.call('ZCARD', key)
    redis.call('PUBLISH', realtime_key, result)
end

return result
