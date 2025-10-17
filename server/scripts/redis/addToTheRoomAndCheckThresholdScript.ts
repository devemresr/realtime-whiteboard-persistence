export const addToTheRoomAndCheckThreshold = `
-- KEYS[1] = room hash key
-- ARGV[1] = roomId
-- ARGV[2] = data (JSON array string)
-- ARGV[3] = current timestamp (ms)
-- ARGV[4] = strokeCountThreshold
-- ARGV[5] = activeRoomsSetKey

local inflightHashKey = KEYS[1]
local inflightMetadataHashKey = KEYS[2]
local roomId = ARGV[1]
local data = ARGV[2]
local now = tonumber(ARGV[3])
local threshold = tonumber(ARGV[4])
local activeRoomsSetKey = ARGV[5]
local parsedData = cjson.decode(data)

-- Check if room exists
local roomExists = redis.call("EXISTS", inflightMetadataHashKey)

-- If not exists, create initial metadata
if roomExists == 0 then
    redis.call("HSET", inflightMetadataHashKey,
        "strokeCount", 0,
        "createdAt", now,
        "lastUpdated", now,
        "isTimedOut", "false"
    )
    redis.call("SADD", activeRoomsSetKey, roomId)
    redis.call("EXPIRE", activeRoomsSetKey, 3600)
end

-- Increment stroke count by batch size
local batchSize = #parsedData
local newCount = redis.call("HINCRBY", inflightMetadataHashKey, "strokeCount", batchSize)
redis.call("HSET", inflightMetadataHashKey, "lastUpdated", now)

-- Add each stroke to the hash
for i, element in ipairs(parsedData) do
    redis.call('HSET', inflightHashKey, tostring(element.packageId), cjson.encode(element))
end

-- Ensure activeRooms is kept alive
redis.call("SADD", activeRoomsSetKey, roomId)
redis.call("EXPIRE", activeRoomsSetKey, 3600)

-- Check threshold
local shouldTrigger = 0
if newCount >= threshold then
    shouldTrigger = 1
end

return { newCount, shouldTrigger }
`;
