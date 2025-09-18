import PersistenceController from './controllers/persistenceController';
import { EventEmitterFactory } from './events/EventEmitterFactory';
import HeartbeatService from './services/heartbeat/HeartbeatService';
import { RedisFactory } from './services/redis/RedisFactory';
import RedisStreamManager from './services/redis/RedisStreamManager';
import { REDIS_STREAM_NAMES } from './shared/constants/socketIoConstants';

export async function bootstrapApplication(port: string): Promise<any> {
	try {
		console.log('Initializing application dependencies...');

		// Create eventEmitter dependency
		const eventEmitterFactory = new EventEmitterFactory();
		console.log('EventEmitterFactory created');

		// Create redis intances
		const redisInstanceForStreams = await RedisFactory.createClient(
			{ port: 6379 },
			'redisForStreams'
		);
		await redisInstanceForStreams.connect();
		redisInstanceForStreams.on('error', (err) =>
			console.error('redisInstanceForStreams', err)
		);

		const redisInstanceForCache = await RedisFactory.createClient(
			{ port: 6380 },
			'redisForCache'
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
			REDIS_STREAM_NAMES.DRAWING_EVENT,
			'processingServersTest'
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
			{ consumerGroup: 'processingServersTest' },
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
