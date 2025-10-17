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
import { STREAM_EVENTS } from './constants/streamEventEmitterConstants';

export async function bootstrapApplication(port: string): Promise<any> {
	try {
		console.log('Initializing application dependencies...');

		// Create eventEmitter dependency
		const eventEmitterFactory = new EventEmitterFactory();
		console.log('EventEmitterFactory created');
		const streamEventEmitter = eventEmitterFactory.createOrGetStreamEvents(
			STREAM_EVENTS,
			3 * 1000
		);

		// Create redis intances
		const redisMain = await RedisFactory.createClient(
			{ port: 6379 },
			REDIS_CLIENTS.MAIN
		);
		redisMain.on('error', (err) => console.error('redisMain', err));

		// await redisMain.getClient().flushall();

		// Create and initialize Redis stream manager
		const redisStreamManager = new RedisStreamManager(
			streamEventEmitter,
			redisMain.getClient()
		);
		await redisStreamManager.createConsumerGroup(
			REDIS_STREAMS.DRAWING_EVENTS,
			REDIS_CONSUMER_GROUPS.PERSISTENCE
		);
		console.log('RedisStreamManager initialized');

		// Initialize heartbeat
		const heartbeatInstance = HeartbeatService.getInstance(
			redisMain.getClient(),
			{
				port,
			}
		);

		// Create persistence controller with explicit dependencies
		const persistenceController = new PersistenceController(
			port,
			streamEventEmitter,
			{ consumerGroup: REDIS_CONSUMER_GROUPS.PERSISTENCE },
			redisStreamManager,
			heartbeatInstance,
			redisMain.getClient()
		);

		await persistenceController.initialize();
		console.log('PersistenceController initialized');

		return {
			persistenceController,
			streamEventEmitter,
			redisStreamManager,
			redisMain,
			heartbeatInstance,
		};
	} catch (error) {
		console.log('error at startup:', error);
		throw error;
	}
}
