import { Redis } from 'ioredis';
import { getActiveServers } from '../scripts/redis/getActiveServersScript';
import heartbeatScript from '../scripts/redis/heartbeatScript';
import { parseRedisFields } from '../utils /parseRedisFields';

export interface ServerInfo {
	id: string;
	timestamp: number;
	status: 'healthy' | 'unhealthy';
	port: string | number;
	startupTime: number;
}

interface ActiveServersResult {
	activeServers: ServerData[];
	removedServers: ServerData[];
}

interface ServerData {
	id: string;
	timestamp: number;
	port: string;
	status: string;
	startupTime: string;
}

interface config {
	port: string | number;
	serverId?: string;
	HEARTBEAT_INTERVAL_MS?: number;
	SERVER_TIMEOUT_SECONDS?: number;
	ORDERED_KEY?: string;
	DATA_KEY?: string;
}

class HeartbeatService {
	private redis: Redis;
	private static instance: HeartbeatService | null = null;
	private serverId: string;

	private port: string | number;
	private serverStartupTime: number;
	private heartbeatInterval: NodeJS.Timeout | null = null;

	private HEARTBEAT_INTERVAL_MS: number;
	private SERVER_TIMEOUT_SECONDS: number;
	private ORDERED_KEY: string;
	private DATA_KEY: string;
	private isStarted: boolean = false;

	private constructor(config: config, redisInstanceForCache: Redis) {
		this.redis = redisInstanceForCache;
		this.port = config.port;
		this.serverId = config.serverId || `server-${config.port}`;
		this.serverStartupTime = Date.now();
		this.HEARTBEAT_INTERVAL_MS = config?.HEARTBEAT_INTERVAL_MS ?? 10 * 1000;
		this.SERVER_TIMEOUT_SECONDS = config?.SERVER_TIMEOUT_SECONDS ?? 10;
		this.ORDERED_KEY = config?.ORDERED_KEY ?? 'active_servers_ordered';
		this.DATA_KEY = config?.DATA_KEY ?? 'active_servers_data';
		this.sendHeartbeat = this.sendHeartbeat.bind(this);
	}

	static getInstance(
		redisInstanceForCache: Redis,
		config?: config
	): HeartbeatService {
		console.log('the config in heartbeatInstance', config);

		if (!HeartbeatService.instance) {
			if (!config) {
				throw new Error(
					'HeartbeatService must be initialized with config on first call'
				);
			}
			HeartbeatService.instance = new HeartbeatService(
				config,
				redisInstanceForCache
			);
		}
		return HeartbeatService.instance;
	}

	/**
	 * Start the heartbeat service
	 */
	async start(): Promise<void> {
		try {
			// Send initial heartbeat immediately
			await this.sendHeartbeat();

			// Start heartbeat interval
			this.heartbeatInterval = setInterval(() => {
				this.sendHeartbeat();
			}, this.HEARTBEAT_INTERVAL_MS);

			console.log(`Heartbeat service started for ${this.serverId}`);
			this.isStarted = true;
		} catch (error) {
			console.error('Failed to start heartbeat service:', error);
			throw error;
		}
	}

	/**
	 * Stop the heartbeat service
	 */
	async stop(): Promise<void> {
		try {
			// Clear intervals
			if (this.heartbeatInterval) {
				clearInterval(this.heartbeatInterval);
				this.heartbeatInterval = null;
			}

			// Remove this server from active list
			await this.removeServer();

			console.log(`Heartbeat service stopped for ${this.serverId}`);
		} catch (error) {
			console.error('Failed to stop heartbeat service:', error);
		}
	}

	/**
	 * Send heartbeat to Redis
	 */
	private async sendHeartbeat() {
		try {
			const timestamp = Math.floor(Date.now() / 1000);
			const serverInfo: ServerInfo = {
				id: this.serverId,
				timestamp,
				status: 'healthy',
				port: this.port,
				startupTime: this.serverStartupTime,
			};

			const rawResults = (await this.redis.eval(
				heartbeatScript,
				2,
				this.ORDERED_KEY,
				this.DATA_KEY,
				this.serverStartupTime.toString(),
				this.port,
				JSON.stringify(serverInfo),
				this.SERVER_TIMEOUT_SECONDS
			)) as string;
			const results = JSON.parse(rawResults);

			// todo incase theres a removed server alert via slack
		} catch (error) {
			console.error('Failed to send heartbeat:', error);
			return [];
		}
	}

	async getActiveServers(): Promise<ActiveServersResult> {
		try {
			if (!this.isStarted) {
				console.warn('HeartbeatService not started yet, returning empty array');
				return {
					activeServers: [] as ServerData[],
					removedServers: [] as ServerData[],
				};
			}
			const rawResults = (await this.redis.eval(
				getActiveServers,
				2,
				this.ORDERED_KEY,
				this.DATA_KEY,
				this.SERVER_TIMEOUT_SECONDS
			)) as string;

			const result = parseRedisFields(
				JSON.parse(rawResults)
			) as ActiveServersResult;

			return result;
		} catch (error) {
			console.error('Failed to get active servers:', error);
			return {
				activeServers: [] as ServerData[],
				removedServers: [] as ServerData[],
			};
		}
	}

	/**
	 * Remove this server from the active list
	 */
	private async removeServer(): Promise<void> {
		try {
			const removeScript = `
				redis.call('ZREM', KEYS[1], ARGV[1])
				redis.call('HDEL', KEYS[2], ARGV[1])
				return redis.call('ZCARD', KEYS[1])
			`;

			await this.redis.eval(
				removeScript,
				2,
				this.ORDERED_KEY,
				this.DATA_KEY,
				this.serverId
			);
		} catch (error) {
			console.error('Failed to remove server:', error);
		}
	}

	/**
	 * Get server info
	 */
	getServerInfo(): { id: string; port: string | number; startupTime: number } {
		return {
			id: this.serverId,
			port: this.port,
			startupTime: this.serverStartupTime,
		};
	}
}

export default HeartbeatService;
