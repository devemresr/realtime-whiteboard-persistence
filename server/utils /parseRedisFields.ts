import { RedisMessage } from '../services/RedisStreamManager';

/**
 * Safely parse a JSON string, returning the original value if parsing fails
 */
function safeJsonParse(value: string): any {
	try {
		return JSON.parse(value);
	} catch {
		return value;
	}
}

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
			obj[key] = safeJsonParse(value);
		}
	} else if (typeof fields === 'object' && fields !== null) {
		// Some Redis clients return an object directly { key: value, ... }
		for (const [key, value] of Object.entries(fields)) {
			if (Array.isArray(value)) {
				// Handle arrays of JSON strings (from Lua scripts)
				obj[key] = value.map((item) => safeJsonParse(item));
			} else {
				obj[key] = safeJsonParse(value);
			}
		}
	}

	return obj;
}

export { parseRedisFields };
