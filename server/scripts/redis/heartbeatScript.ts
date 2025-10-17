import { getActiveServersWithoutReturn } from './getActiveServersScript';

const heartbeatScript = `
				-- Store server in ordered set (serverId as member, serverStartupTime as score)
                local orderedActiveServerListName = KEYS[1]
                local activeServerListName = KEYS[2]
                local serverStartupTime = tonumber(ARGV[1])
                local serverId = ARGV[2]
				local serverStatus = ARGV[3]
				local cleanupTime = tonumber(ARGV[4])
				local currentTime = redis.call('TIME')[1]
				local cutoff = currentTime - (cleanupTime + 1)
				redis.call('ZADD', orderedActiveServerListName, serverStartupTime, serverId)
				local indexedActiveServers = redis.call('ZRANGEBYSCORE', orderedActiveServerListName, '-inf', '+inf')
				
				
				-- Store full server data in hash
				redis.call('HSET', activeServerListName, serverId, serverStatus)

                ${getActiveServersWithoutReturn}
				
				local resultObj = {
    			serverIds = serverIds,
    			activeServers = activeServers,
    			removedServers = removedServers,
    			currentTime = currentTime,
    			cutoff = cutoff,
				indexedActiveServers = indexedActiveServers
				}

				return cjson.encode(resultObj)`;
export default heartbeatScript;
