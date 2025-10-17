const getInflightRoomDataScript = `
-- KEYS[1] = inflightHashKey 
local inflightHashKey = KEYS[1]

-- Get all strokes from set
local strokesData = redis.call("HGETALL", inflightHashKey)

return strokesData
`;

export default getInflightRoomDataScript;
