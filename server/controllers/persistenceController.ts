import Redis from 'ioredis';
import RedisStreamManager from '../services/redis/RedisStreamManager';
import { REDIS_STREAM_NAMES } from '../shared/constants/socketIoConstants';
import RoomData, { RoomDataBase } from '../schemas/Strokes';
import addToTheRoomAndCheckThreshold from '../scripts/redis/addToTheRoomAndCheckThresholdScript';
import getRoomBatchDataScript from '../scripts/redis/getRoomBatchDataScript';
import cleanupProcessedStrokesScript from '../scripts/redis/cleanupProcessedStrokesScript';
import stableHashNumeric from '../utils /stableHash';
import checkAndDetermineTimeout from '../scripts/redis/checkAndDetermineTimeoutScript';
import HeartbeatService from '../services/heartbeat/HeartbeatService';
import { StreamEvents } from '../events/StreamEvents';
import { EventEmitterFactory } from '../events/EventEmitterFactory';

interface BatchProcessingConfig {
	strokeCountThreshold: number;
	timeoutMs: number;
	consumerGroup: string;
}
interface BatchProcessingInput {
	strokeCountThreshold?: number;
	timeoutMs?: number;
	consumerGroup: string;
}

interface RoomBatchData {
	strokeCount: number;
	strokesData: string[];
	isTimedOut: boolean;
}

class PersistenceController {
	private readonly config: BatchProcessingConfig;
	private isInitialized = false;
	private heartBeatService: HeartbeatService;
	private PORT;
	private streamEventEmitter: StreamEvents;
	private redis: Redis;
	private redisStreamManager: RedisStreamManager;

	constructor(
		PORT: string,
		eventEmitterFactory: EventEmitterFactory,
		config: BatchProcessingInput,
		redisStreamManager: RedisStreamManager,
		heartBeatService: HeartbeatService,
		redis: Redis
	) {
		this.redis = redis;
		this.redisStreamManager = redisStreamManager;
		this.PORT = PORT;
		this.heartBeatService = heartBeatService;
		this.streamEventEmitter = eventEmitterFactory.createOrGetStreamEvents(
			'streamEvents',
			30 * 1000
		);

		this.config = {
			strokeCountThreshold: 100,
			timeoutMs: 3 * 1000, // after timeout runs out and if it didnt hit to threshold amount we batchwrite it
			...config,
		};
	}

	public async initialize(): Promise<void> {
		if (this.isInitialized) return;

		try {
			console.log('Redis streams initialized');

			this.redisStreamManager.consumeFromGroup(
				REDIS_STREAM_NAMES.DRAWING_EVENT,
				this.config.consumerGroup,
				this.handleDrawingEvent.bind(this),
				{ timeoutHandler: this.processTimedOutRooms.bind(this) }
			);

			this.isInitialized = true;
			console.log('DrawingPersistenceManager initialized successfully');
		} catch (error) {
			console.error('Failed to initialize DrawingPersistenceManager:', error);
			throw error;
		}
	}

	// ========== Event Handlers ==========

	private async handleDrawingEvent(data: any): Promise<void> {
		try {
			if (!this.isInitialized) {
				console.error('the PersistenceController hasnt been initialized yet');
				return;
			}
			const roomId = data.roomId;

			await this.addStrokeToRoom(roomId, data);
		} catch (error) {
			console.error('Error handling drawing event:', error);
			throw error;
		}
	}

	// ========== Room Management ==========
	private async addStrokeToRoom(
		roomId: string,
		strokeData: any
	): Promise<void> {
		try {
			const roomKey = this.getRoomKey(roomId);
			const strokesKey = this.getRoomStrokesKey(roomId);
			const now = Date.now();

			// This call also set the ActiveRooms
			const shouldTriggerBatchWriteResult = (await this.redis.eval(
				addToTheRoomAndCheckThreshold,
				2, // number of KEYS
				roomKey,
				strokesKey,
				roomId,
				JSON.stringify(strokeData),
				now.toString(),
				this.config.strokeCountThreshold
			)) as [number, number];

			const shouldTriggerBatchWrite = shouldTriggerBatchWriteResult[1];
			const strokeCount = shouldTriggerBatchWriteResult[0];
			console.log(
				'added to the room: ',
				roomId,
				'the packageId: ',
				strokeData.packageId,
				'the rooms',
				roomId,
				'stroke count: ',
				strokeCount
			);
			if (shouldTriggerBatchWrite) {
				await this.acquireLock(roomId);
			}
		} catch (error) {
			console.error(`Error adding stroke to room ${roomId}:`, error);
			throw error;
		}
	}

	private async processTimedOutRooms() {
		try {
			if (!this.isInitialized)
				return 'the PersistenceController hasnt been initialized yet';
			const activeRoomIds = await this.getActiveRoomIds();
			console.log('activeRoomIds', activeRoomIds);

			if (activeRoomIds.length === 0) {
				console.log('no active rooms to check');
				return;
			}

			for (let activeRoomId of activeRoomIds) {
				await this.checkAndProcessTimedOutRoom(activeRoomId);
			}
		} catch (error) {
			console.error('Error processing timed-out rooms:', error);
			return false;
		}
	}

	private async isMyRoom(roomId: string) {
		try {
			const serverStatus = await this.getActiveServers();
			const { activeServers } = serverStatus;
			if (activeServers.length === 0) return null;

			const myserverIndex = activeServers.findIndex(
				(server) => server.port === this.PORT
			);
			if (myserverIndex === -1) {
				console.log('this server failed to give heartbeat');
				return;
			}

			const cycle_number = Math.floor(Date.now() / (10 * 1000));
			console.log('cycle_number', cycle_number);
			const responsibleIndex =
				stableHashNumeric(`${roomId}-${cycle_number}`) % activeServers.length;

			return responsibleIndex === myserverIndex;
		} catch (error) {
			console.error('Error determining server responsibility:', error);
			return false;
		}
	}

	private async checkAndProcessTimedOutRoom(roomId: string): Promise<void> {
		const maxRetries = 3;
		for (let attempt = 0; attempt < maxRetries; attempt++) {
			try {
				// Watch for server topology changes
				await this.redis.watch('active_servers_ordered', 'active_servers_data');

				// Check if we're responsible (outside transaction)
				const isResponsible = await this.isMyRoom(roomId);
				if (!isResponsible) {
					console.log('isnt responsible for the room: ', roomId);
					await this.redis.unwatch();
					return;
				}
				console.log('serverId', this.PORT, 'isResponsible for:', roomId);
				const roomKey = this.getRoomKey(roomId);
				const strokesKey = this.getRoomStrokesKey(roomId);

				const timeoutResult = (await this.redis.eval(
					checkAndDetermineTimeout,
					2,
					roomKey,
					strokesKey,
					Date.now().toString(),
					this.config.timeoutMs.toString()
				)) as [number, string];

				const [shouldProcess, strokeCount] = timeoutResult;

				if (shouldProcess !== 1) {
					await this.redis.unwatch();
					return;
				}
				console.log(
					'is the room timed out: ',
					shouldProcess === 1 ? true : false,
					'the rooms id:',
					roomId,
					'stroke count: ',
					strokeCount
				);

				// Execute empty transaction to detect changes
				console.log(
					'before ',
					await this.redis.zrangebyscore(
						'active_servers_ordered',
						'-inf',
						'+inf'
					)
				);

				// await this.redis.zremrangebyrank('active_servers_ordered', 1, 1);
				const multi = this.redis.multi();
				const result = await multi.exec();
				// console.log('result', result);
				// console.log(
				// 	'after ',
				// 	await this.redis.zrangebyscore(
				// 		'active_servers_ordered',
				// 		'-inf',
				// 		'+inf'
				// 	)
				// );

				if (result === null) {
					// Server list changed - our calculations are invalid, retry
					console.log('topology changed');
					continue;
				}

				// Server topology didn't change try to acquire lock
				const lockAcquired = await this.acquireLock(roomId);
				if (!lockAcquired) {
					return; // Exit cleanly - another server is handling it
				}

				return;
			} catch (error) {
				console.error(
					`Process room ${roomId} attempt ${attempt + 1} failed:`,
					error
				);
				await this.redis.unwatch();
				if (attempt === maxRetries - 1) {
					console.error(
						`Failed to process room ${roomId} after ${maxRetries} attempts`
					);
				}
			}
		}
	}

	private async acquireLock(roomId: string) {
		const lockKey = `lock:room:${roomId}`;
		const lockValue = `${process.pid}-${Date.now()}`; // unique identifier
		const acquired = await this.redis.set(
			lockKey,
			lockValue,
			'EX',
			30, // 30 second timeout for database operations
			'NX'
		);

		console.log('acquired', acquired);

		if (!acquired) {
			console.log(
				`Room ${roomId} is already being processed by another instance`
			);
			return false; // Another process is handling this room
		}

		// Process batch write asynchronously
		this.processBatchWriteWithLock(roomId, lockKey, lockValue).catch(
			(error: any) => {
				console.error(`Batch write failed for room ${roomId}:`, error);
			}
		);
		return true;
	}

	// ========== Batch Processing ==========
	private async processBatchWriteWithLock(
		roomId: string,
		lockKey: string,
		lockValue: string
	): Promise<void> {
		try {
			// get the roomData via lua script
			const raw = await this.getRoomBatchData(roomId);
			console.log('raw', raw);

			const batchData: RoomBatchData = {
				strokeCount: raw.strokeCount,
				strokesData: raw.strokesData,
				isTimedOut: raw.isTimedOut,
			};

			const validStrokes = this.parseAndValidateStrokes(batchData.strokesData);
			if (validStrokes.length === 0) {
				console.log('No valid strokes to process');
				await this.removeFromActiveRooms(roomId);
				return;
			}

			await this.persistStrokesToDatabase(roomId, validStrokes);
			console.log(
				`Successfully batch processed ${validStrokes.length} strokes for room ${roomId}`
			);
		} catch (error) {
			console.error(`Error in batch processing for room ${roomId}:`, error);
			throw error;
		} finally {
			await this.releaseLockIfOwned(lockKey, lockValue);
		}
	}
	private async releaseLockIfOwned(
		lockKey: string,
		lockValue: string
	): Promise<void> {
		const releaseLockIfOwnedScript = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`;
		try {
			await this.redis.eval(releaseLockIfOwnedScript, 1, lockKey, lockValue);
		} catch (error) {
			console.error('Failed to release lock:', error);
		}
	}

	// ========== Data Retrieval Helpers ==========
	private async getRoomBatchData(roomId: string) {
		const roomKey = this.getRoomKey(roomId);
		const strokesKey = this.getRoomStrokesKey(roomId);

		const res = await this.redis.eval(
			getRoomBatchDataScript,
			2, // number of KEYS
			roomKey,
			strokesKey
		);
		const [strokeCountStr, strokesData, isTimedOutStr] = res as [
			string,
			string[],
			string,
		];

		return {
			strokeCount: parseInt(strokeCountStr || '0'),
			strokesData,
			isTimedOut: isTimedOutStr === '1',
		};
	}

	private parseAndValidateStrokes(strokesData: string[]): RoomDataBase[] {
		return strokesData
			.map((strokeJson) => {
				try {
					return JSON.parse(strokeJson);
				} catch (error) {
					console.error('Failed to parse stroke data:', strokeJson, error);
					return null;
				}
			})
			.filter((stroke): stroke is RoomDataBase => stroke !== null);
	}

	// ========== Database Operations ==========
	private async persistStrokesToDatabase(
		roomId: string,
		strokes: RoomDataBase[]
	): Promise<void> {
		const operations = strokes.map((stroke) => ({
			insertOne: { document: stroke },
		}));

		try {
			const result = await RoomData.bulkWrite(operations, { ordered: false }); // Continue processing even if some operations fail
			console.log('Database bulk insert result:', {
				insertedCount: result.insertedCount,
				matchedCount: result.matchedCount,
				modifiedCount: result.modifiedCount,
				upsertedCount: result.upsertedCount,
				result,
			});

			// Handle partial failures
			const successfulInserts = result.insertedCount || 0;
			const failedInserts = strokes.length - successfulInserts;

			if (failedInserts > 0) {
				console.warn(
					`${failedInserts} out of ${strokes.length} inserts failed for room ${roomId}`
				);

				// Clean up only successful strokes
				await this.cleanupSuccessfulStrokes(roomId, strokes);
			} else {
				// All inserts was successful
				await this.cleanupProcessedStrokes(roomId, strokes);
			}
		} catch (error) {
			console.error('Database bulk insert failed:', error);

			// Don't cleanup any strokes if the entire batch failed
			// They will be retried in the next batch
			throw error;
		}
	}
	private async cleanupSuccessfulStrokes(
		roomId: string,
		originalStrokes: RoomDataBase[]
	): Promise<void> {
		if (originalStrokes.every((stroke) => stroke.packageId)) {
			// if theres packageId in each element is truthy then proceed
			const insertedIds = await this.getInsertedStrokeIds(originalStrokes);

			const successfulStrokes = originalStrokes.filter((stroke) =>
				insertedIds.includes(stroke.packageId)
			);

			// get the successfulStrokes by comparing which stroke has been actually written (mongodb doesnt return which documents was successfuly written so we querry and compare)
			await this.cleanupProcessedStrokes(roomId, successfulStrokes);
			return;
		}
	}

	private async getInsertedStrokeIds(
		strokes: RoomDataBase[]
	): Promise<string[]> {
		const ids = strokes
			.map((stroke: RoomDataBase) => stroke?.packageId)
			.filter(Boolean);

		if (ids.length === 0) return [];

		try {
			const insertedDocs = await RoomData.find({
				packageId: { $in: ids },
			})
				.select('packageId')
				.lean();

			return insertedDocs.map((doc) => doc.packageId.toString());
		} catch (error) {
			console.error('Failed to verify inserted strokes:', error);
			return [];
		}
	}

	private async cleanupProcessedStrokes(
		roomId: string,
		processedStrokes: RoomDataBase[]
	): Promise<void> {
		const roomKey = this.getRoomKey(roomId);
		const strokesKey = this.getRoomStrokesKey(roomId);

		try {
			// Prepare arguments for Lua script
			const strokeJsonStrings = processedStrokes.map((stroke) =>
				JSON.stringify(stroke)
			);

			console.log('processedStrokes', processedStrokes);

			const args = [
				processedStrokes.length.toString(),
				Date.now().toString(),
				...strokeJsonStrings,
			];

			const result = (await this.redis.eval(
				cleanupProcessedStrokesScript,
				2, // Number of KEYS
				roomKey,
				strokesKey,
				...args
			)) as [number, number, number];

			const [removedCount, newCount, previousCount] = result;

			console.log(`Cleanup results for room ${roomId}:`, {
				processedStrokes: processedStrokes.length,
				actuallyRemoved: removedCount,
				previousCount,
				newCount,
			});

			const persistedMessageIds = processedStrokes.map((i) => {
				return i.redisMessageId;
			});

			this.streamEventEmitter.emitPersistedPackages(persistedMessageIds);

			// Remove room from active rooms if no strokes left
			if (newCount === 0) {
				await this.removeFromActiveRooms(roomId);
			}
		} catch (error) {
			console.error(
				`Failed to cleanup processed strokes for room ${roomId}:`,
				error
			);
			throw error;
		}
	}

	private cleanup() {}

	// ========== Redis Helper Methods ==========

	private getRoomKey(roomId: string): string {
		return `room:${roomId}`;
	}

	private getRoomStrokesKey(roomId: string): string {
		return `room:${roomId}:strokes`;
	}

	private async getActiveRoomIds(): Promise<string[]> {
		return await this.redis.smembers('activeRooms');
	}

	private async removeFromActiveRooms(roomId: string): Promise<void> {
		await this.redis.srem('activeRooms', roomId);
	}

	private async getActiveServers() {
		return await this.heartBeatService.getActiveServers();
	}
}

export default PersistenceController;
