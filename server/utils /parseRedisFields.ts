import { RedisMessage } from '../services/RedisStreamManager';

/**
 * Convert Redis fields array to JavaScript object or redis object to JavaScript object
 */
function parseRedisFields(
	fields: string[] | Record<string, string> | string
): RedisMessage {
	const obj: RedisMessage = {};

	if (Array.isArray(fields)) {
		// HGETALL returns an array like [key1, val1, key2, val2, ...]
		for (let i = 0; i < fields.length; i += 2) {
			const key = fields[i];
			const value = fields[i + 1];
			try {
				obj[key] = JSON.parse(value);
			} catch {
				obj[key] = value;
			}
		}
	} else if (typeof fields === 'object' && fields !== null) {
		// Some Redis clients return an object directly { key: value, ... }
		for (const [key, value] of Object.entries(fields)) {
			try {
				obj[key] = JSON.parse(value);
			} catch {
				obj[key] = value;
			}
		}
	}

	return obj;
}

export { parseRedisFields };
