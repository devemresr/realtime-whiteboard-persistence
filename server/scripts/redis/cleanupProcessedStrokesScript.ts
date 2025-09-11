const cleanupProcessedStrokesScript = `
-- KEYS[1] = roomKey (hash)
-- KEYS[2] = strokesKey (set)
-- ARGV[1] = processedStrokesCount
-- ARGV[2] = currentTimestamp
-- ARGV[3...] = JSON strings of processed strokes to remove

local roomKey = KEYS[1]
local strokesKey = KEYS[2]
local processedCount = tonumber(ARGV[1])
local currentTimestamp = ARGV[2]

-- Remove processed strokes from the set
local removedCount = 0
for i = 3, #ARGV do
    local removed = redis.call('SREM', strokesKey, ARGV[i])
    removedCount = removedCount + removed
end

-- Get current stroke count
local currentCount = tonumber(redis.call('HGET', roomKey, 'strokeCount') or '0')

-- Calculate new count (ensure it doesn't go below 0)
local newCount = math.max(0, currentCount - removedCount)

-- Update room metadata
redis.call('HSET', roomKey, 
    'strokeCount', tostring(newCount),
    'lastProcessed', currentTimestamp,
    'isTimedOut', '0'
)

-- Return useful information
return {
    removedCount,
    newCount,
    currentCount
}`;

export default cleanupProcessedStrokesScript;
