-- KEYS: [QueueName]

-- first get what is the active queue
local result = redis.call("zrange", KEYS[1] .. ":active", 0, 0, "WITHSCORES")

-- nothing? return nothing
local group = result[1]
if not group then
    return {"empty", ""}
end

local queue = KEYS[1] .. ":" .. group

-- pop off our queue
local result = redis.call("zrangebyscore", queue, 0, "+inf", "WITHSCORES", "LIMIT", 0, 1)

-- found a result?
if result[1] then
    -- then remove it from the queue
    redis.call('zremrangebyrank', queue, 0, 0)

    -- and add a worker to this queue
    redis.call("zincrby", KEYS[1] .. ":active", 1, group)

    return {group, result[1]}
else
    -- no result found, remove this group from active queues
    redis.call("zrem", KEYS[1] .. ":active", group)

    return {"retry", ""}
end
