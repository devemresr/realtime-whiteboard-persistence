import Redis, { RedisOptions } from 'ioredis';
import { EventEmitter } from 'events';

export class RedisClient extends EventEmitter {
	private client: Redis;
	private config: RedisOptions;
	private isConnected = false;
	private connectionPromise: Promise<void> | null = null;

	constructor(config?: RedisOptions) {
		super();

		this.config = {
			host: process.env.REDIS_HOST || 'localhost',
			port: parseInt(process.env.REDIS_PORT || '6380'),
			maxRetriesPerRequest: 3,
			connectTimeout: 10000,
			keepAlive: 30000,
			enableReadyCheck: true,
			...config,
		};

		this.client = new Redis(this.config);
		this.setupEventHandlers();
	}

	private setupEventHandlers(): void {
		this.client.on('connecting', () => {
			console.log('Connecting to Redis...');
			this.emit('connecting');
		});

		this.client.on('connect', () => {
			console.log('Connected to Redis');
			this.emit('connect');
		});

		this.client.on('ready', () => {
			this.isConnected = true;
			console.log('Redis ready');
			this.emit('ready');
		});

		this.client.on('error', (err) => {
			this.isConnected = false;
			console.error('Redis connection error:', err);
			this.emit('error', err);
		});

		this.client.on('close', () => {
			this.isConnected = false;
			console.log('Redis connection closed');
			this.emit('close');
		});

		this.client.on('reconnecting', () => {
			console.log('Redis reconnecting...');
			this.emit('reconnecting');
		});

		this.client.on('end', () => {
			this.isConnected = false;
			console.log('Redis connection ended');
			this.emit('end');
		});
	}

	async connect(): Promise<void> {
		if (this.connectionPromise) {
			return this.connectionPromise;
		}

		if (this.isConnected) {
			return Promise.resolve();
		}

		this.connectionPromise = new Promise<void>((resolve, reject) => {
			const onReady = () => {
				cleanup();
				resolve();
			};

			const onError = (err: Error) => {
				cleanup();
				reject(err);
			};

			const cleanup = () => {
				this.removeListener('ready', onReady);
				this.removeListener('error', onError);
				this.connectionPromise = null;
			};

			this.once('ready', onReady);
			this.once('error', onError);

			if (this.isConnected) {
				cleanup();
				resolve();
			}
		});

		return this.connectionPromise;
	}

	async waitForConnection(timeoutMs: number = 10000): Promise<void> {
		if (this.isConnected) return;

		return Promise.race([
			this.connect(),
			new Promise<never>((_, reject) => {
				setTimeout(() => {
					reject(new Error(`Redis connection timeout after ${timeoutMs}ms`));
				}, timeoutMs);
			}),
		]);
	}

	getClient(): Redis {
		if (!this.isReady()) {
			throw new Error('Redis client not ready. Call connect() first.');
		}
		return this.client;
	}

	isReady(): boolean {
		return this.isConnected && this.client.status === 'ready';
	}

	async ping(): Promise<string> {
		if (!this.isConnected) throw new Error('Redis not connected');
		return this.client.ping();
	}

	async healthCheck() {
		try {
			const ping = await this.ping();
			return {
				connected: this.isConnected,
				status: this.client.status,
				ping,
			};
		} catch (error) {
			return {
				connected: false,
				status: this.client.status,
				error: error instanceof Error ? error.message : 'Unknown error',
			};
		}
	}

	async disconnect(): Promise<void> {
		if (this.client) {
			this.isConnected = false;
			await this.client.disconnect();
		}
	}

	getConfig(): RedisOptions {
		return { ...this.config };
	}
}
