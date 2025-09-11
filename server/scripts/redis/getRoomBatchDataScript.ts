const getRoomBatchDataScript = `
-- KEYS[1] = room hash key
-- KEYS[2] = strokes set key

local roomKey = KEYS[1]
local strokesKey = KEYS[2]

-- Get strokeCount and isTimedOut from hash
local strokeCount = redis.call("HGET", roomKey, "strokeCount")
local isTimedOut = redis.call("HGET", roomKey, "isTimedOut")

-- Get all strokes from set
local strokesData = redis.call("SMEMBERS", strokesKey)

-- Convert nils to defaults
if not strokeCount then strokeCount = "0" end
if not isTimedOut then isTimedOut = "0" end

-- Return as an array: {strokeCount, strokesData (table), isTimedOut}
return {strokeCount, strokesData, isTimedOut}
`;

export default getRoomBatchDataScript;
