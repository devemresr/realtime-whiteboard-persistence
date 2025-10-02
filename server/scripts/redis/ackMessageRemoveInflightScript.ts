export const ackMessageRemoveInflightScript = `
-- KEYS[1] = stream
-- KEYS[2] = group  
-- ARGV[1] = JSON string of messageIds array with {redisMessageId, roomId} objects

local stream = KEYS[1]
local group = KEYS[2]
local messageIdsJson = ARGV[1]
local roomId = ARGV[2]
local messageIds = {}

-- Parse the JSON array
local messageIdsArray = cjson.decode(messageIdsJson)

-- Process each message ID
for i, messageId in ipairs(messageIdsArray) do
    -- Build arrays for bulk operations
    table.insert(messageIds, messageId)
end

-- Acknowledge all messages in the stream
local ackResult = redis.call('XACK', stream, group, unpack(messageIds))

-- Remove messages from the inFlight set and add to persisted set
local inflightRemovedCount = redis.call('SREM', 'inFlight-' .. roomId, unpack(messageIds))
local persistedAddedCount = redis.call('SADD', 'persisted-' .. roomId, unpack(messageIds))

-- Clean up any empty inflight sets
local count = redis.call('SCARD', 'inFlight-' .. roomId)
if count == 0 then
    redis.call('DEL', 'inFlight-' .. roomId)
end

local result = {
messageIds = messageIds,
ackResult = ackResult,
inflightRemovedCount = inflightRemovedCount,
persistedAddedCount = persistedAddedCount
}

return cjson.encode(result)
`;
