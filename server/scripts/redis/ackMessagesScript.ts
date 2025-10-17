export const ackMessagesScript = `
-- KEYS[1] = stream
-- KEYS[2] = group  
-- ARGV[1] = JSON string of messageIds array with {redisMessageId, roomId} objects

local stream = KEYS[1]
local group = KEYS[2]
local messageIdsJson = ARGV[1]
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

local result = {
messageIds = messageIds,
ackResult = ackResult,
}

return cjson.encode(result)
`;
