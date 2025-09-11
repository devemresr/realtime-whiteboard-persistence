const addToTheRoomAndCheckThreshold = `
-- KEYS[1] = room hash key
-- KEYS[2] = strokes set key
-- ARGV[1] = roomId
-- ARGV[2] = strokeData (JSON string)
-- ARGV[3] = current timestamp (ms)
-- ARGV[4] = strokeCountThreshold

local roomKey = KEYS[1]
local strokesKey = KEYS[2]
local roomId = ARGV[1]
local strokeData = ARGV[2]
local now = tonumber(ARGV[3])
local threshold = tonumber(ARGV[4])

-- Check if room exists
local roomExists = redis.call("EXISTS", roomKey)

-- If not exists, create initial metadata
if roomExists == 0 then
    redis.call("HSET", roomKey,
        "roomId", roomId,
        "strokeCount", 0,
        "createdAt", now,
        "lastUpdated", now,
        "isTimedOut", "false"
    )
    redis.call("SADD", "activeRooms", roomId)
    redis.call("EXPIRE", "activeRooms", 3600) -- 1 hour TTL
end

-- Increment stroke count & update metadata
local newCount = redis.call("HINCRBY", roomKey, "strokeCount", 1)
redis.call("HSET", roomKey, "lastUpdated", now)

-- Add stroke data to strokes set
redis.call("SADD", strokesKey, strokeData)

-- Ensure activeRooms is kept alive
redis.call("SADD", "activeRooms", roomId)
redis.call("EXPIRE", "activeRooms", 3600)

-- Check threshold
local shouldTrigger = 0
if newCount >= threshold then
    shouldTrigger = 1
end

-- return [new stroke count, shouldTrigger flag]
return { newCount, shouldTrigger }
`;

export default addToTheRoomAndCheckThreshold;
