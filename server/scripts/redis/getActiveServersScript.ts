export const getActiveServersWithoutReturn = `
local serverIds = redis.call('ZRANGE', orderedActiveServerListName, 0, -1)				
local activeServers = {}
local removedServers = {}

for i = 1, #serverIds do
    local serverDataStr = redis.call('HGET', activeServerListName, serverIds[i])
    if serverDataStr then
        local serverData = cjson.decode(serverDataStr)
        if serverData.timestamp > cutoff then
            activeServers[#activeServers + 1] = cjson.encode(serverData)
        else
		-- one or more of the servers failed to give a heartbeat removing server
		redis.call('ZREM', orderedActiveServerListName, serverIds[i])
		redis.call('HDEL', activeServerListName, serverIds[i])
		removedServers[#removedServers + 1 ] = cjson.encode(serverData)
		end
	end
end`;

export const getActiveServers = `
local orderedActiveServerListName = KEYS[1]
local activeServerListName = KEYS[2]
local cleanupTime = tonumber(ARGV[1])
local currentTime = redis.call('TIME')[1]
local cutoff = currentTime - (cleanupTime + 1)

${getActiveServersWithoutReturn} 
local resultObj = {
activeServers= activeServers,
removedServers= removedServers
}
return cjson.encode(resultObj)
`;
