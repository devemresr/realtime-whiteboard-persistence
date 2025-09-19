import PersistenceController from './controllers/persistenceController';
import { EventEmitterFactory } from './events/EventEmitterFactory';
import HeartbeatService from './services/heartbeat/HeartbeatService';
import { RedisFactory } from './services/redis/RedisFactory';
import RedisStreamManager from './services/redis/RedisStreamManager';
import {
	REDIS_CLIENTS,
	REDIS_CONSUMER_GROUPS,
	REDIS_STREAMS,
} from './constants/RedisConstants';

export async function bootstrapApplication(port: string): Promise<any> {
	try {
		console.log('Initializing application dependencies...');

		// Create eventEmitter dependency
		const eventEmitterFactory = new EventEmitterFactory();
		console.log('EventEmitterFactory created');

		// Create redis intances
		const redisInstanceForStreams = await RedisFactory.createClient(
			{ port: 6379 },
			REDIS_CLIENTS.STREAM
		);
		await redisInstanceForStreams.connect();
		redisInstanceForStreams.on('error', (err) =>
			console.error('redisInstanceForStreams', err)
		);

		const redisInstanceForCache = await RedisFactory.createClient(
			{ port: 6380 },
			REDIS_CLIENTS.CACHE
		);
		await redisInstanceForCache.connect();
		redisInstanceForCache.on('error', (err) =>
			console.error('redisInstanceForCache', err)
		);

		// Create and initialize Redis stream manager
		const redisStreamManager = new RedisStreamManager(
			eventEmitterFactory,
			redisInstanceForStreams.getClient()
		);
		await redisStreamManager.createConsumerGroup(
			REDIS_STREAMS.DRAWING_EVENTS,
			REDIS_CONSUMER_GROUPS.PERSISTENCE
		);
		console.log('RedisStreamManager initialized');

		// Initialize heartbeat
		const heartbeatInstance = HeartbeatService.getInstance(
			redisInstanceForCache.getClient(),
			{ port }
		);

		// Create persistence controller with explicit dependencies
		const persistenceController = new PersistenceController(
			port,
			eventEmitterFactory,
			{ consumerGroup: REDIS_CONSUMER_GROUPS.PERSISTENCE },
			redisStreamManager,
			heartbeatInstance,
			redisInstanceForStreams.getClient()
		);

		await persistenceController.initialize();
		console.log('PersistenceController initialized');

		return {
			persistenceController,
			eventEmitterFactory,
			redisStreamManager,
			redisInstanceForCache,
			heartbeatInstance,
			redisInstanceForStreams,
		};
	} catch (error) {
		console.log('error at startup:', error);
		throw error;
	}
}
