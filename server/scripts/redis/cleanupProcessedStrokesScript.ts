const cleanupProcessedStrokesScript = `
local inflightHashKey = KEYS[1]
local inflightMetadataHashKey = KEYS[2]
local persistedRoomsKey = KEYS[3]
local currentTimestamp = ARGV[1]
local activeRoomsSetKey = ARGV[2]
local processedStrokes = ARGV[3]
local roomId = ARGV[4]
local parsedProcessedStrokes = cjson.decode(processedStrokes)

-- Remove processed strokes from the set
local removedCount = 0
local addedCount = 0
local successfullyProcessedElementsRedisMessageIds = {}

for i, element in ipairs(parsedProcessedStrokes) do
    local removed = redis.call('HDEL', inflightHashKey, tostring(element.packageId))
    local added = redis.call('HSET', persistedRoomsKey, tostring(element.packageId), cjson.encode(element))
    removedCount = removed + removedCount
    addedCount = added + addedCount
    table.insert(successfullyProcessedElementsRedisMessageIds, tostring(element.redisMessageId))
end

-- Get current stroke count
local currentCount = tonumber(redis.call('HGET', inflightMetadataHashKey, 'strokeCount') or '0')

-- Calculate new count (ensure it doesn't go below 0)
local newCount = math.max(0, currentCount - removedCount)

if newCount == 0 then
    -- All strokes persisted, room doenst have any unpersisted data
    redis.call('DEL', inflightMetadataHashKey)
    redis.call('DEL', inflightHashKey)
    redis.call('SREM', activeRoomsSetKey, roomId)
else
    -- All strokes didnt persisted still add to snapshot queue for incremental snapshot
    -- Update room metadata
    redis.call('HSET', inflightMetadataHashKey, 
        'strokeCount', tostring(newCount),
        'lastProcessed', currentTimestamp,
        'isTimedOut', '0'
    )
end


-- Return useful information
return {
    newCount,
    removedCount,
    successfullyProcessedElementsRedisMessageIds
}`;

export default cleanupProcessedStrokesScript;
