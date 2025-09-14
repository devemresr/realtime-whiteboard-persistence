const checkAndDetermineTimeout = `
-- KEYS[1] = roomKey (room:roomId)
-- KEYS[2] = strokesKey (room:roomId:strokes) 
-- ARGV[1] = currentTimestamp
-- ARGV[2] = timeoutMs

local roomKey = KEYS[1]
local strokesKey = KEYS[2]
local currentTime = tonumber(ARGV[1])
local timeoutMs = tonumber(ARGV[2])

-- Get room metadata atomically
local lastUpdated = redis.call('HGET', roomKey, 'lastUpdated')
if not lastUpdated then
    return {0, 'no_room'} -- Room doesn't exist
end

-- Check if timed out
local timeSinceUpdate = currentTime - tonumber(lastUpdated)
if timeSinceUpdate <= timeoutMs then
    return {0, 'not_timed_out'} -- Still active
end

-- Check if has unprocessed strokes
local strokeCount = redis.call('SCARD', strokesKey)
if strokeCount == 0 then
    return {0, 'no_strokes'} -- Nothing to process
end

return {1, strokeCount}`;
export default checkAndDetermineTimeout;
