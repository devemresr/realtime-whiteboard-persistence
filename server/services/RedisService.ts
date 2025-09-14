import Redis, { RedisOptions } from 'ioredis';

class RedisService {
	private static instance: RedisService | null = null;
	private client: Redis;
	private config: RedisOptions;

	private constructor(config?: RedisOptions) {
		const redisConfig: RedisOptions = {
			host: 'localhost',
			port: 6380,
			maxRetriesPerRequest: 3,
			connectTimeout: 10000,
			keepAlive: 30000,
			...config,
		};

		this.client = new Redis(redisConfig);
		this.config = redisConfig; // Store the merged config
	}

	static getInstance(config?: RedisOptions): RedisService {
		if (!RedisService.instance) {
			RedisService.instance = new RedisService(config);
		}
		return RedisService.instance;
	}

	static isInitialized(): boolean {
		return RedisService.instance !== null;
	}

	getClient(): Redis {
		return this.client;
	}

	async disconnect(): Promise<void> {
		if (this.client) {
			await this.client.disconnect();
		}
		RedisService.instance = null;
	}

	getConfig(): RedisOptions {
		return { ...this.config }; // Return a copy to prevent mutation
	}

	async ping(): Promise<string> {
		return this.client.ping();
	}
}

export default RedisService;
