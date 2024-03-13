-- KEYS: [QueueName] [TaskGroup]

-- decrement our active
local active = tonumber(redis.call("zincrby", KEYS[1] .. ":active", -1, KEYS[2]))

-- reset to zero if we somehow go below
if active < 0 then
    redis.call("zadd", KEYS[1] .. ":active", 0, KEYS[2])
end
