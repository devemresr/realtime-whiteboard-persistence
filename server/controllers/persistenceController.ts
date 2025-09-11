import Redis from 'ioredis';
import RedisStreamManager from '../services/RedisStreamManager';
import { REDIS_STREAM_EVENTS } from '../shared/constants/socketIoConstants';
import RoomData, { RoomDataBase } from '../schemas/Strokes';
import addToTheRoomAndCheckThreshold from '../scripts/redis/addToTheRoomAndCheckThreshold';
import getRoomBatchDataScript from '../scripts/redis/getRoomBatchDataScript';
import cleanupProcessedStrokesScript from '../scripts/redis/cleanupProcessedStrokesScript';

interface RoomMetadata {
	roomId: string;
	strokeCount: number;
	createdAt: number;
	lastUpdated: number;
	isTimedOut: boolean;
}

interface BatchProcessingConfig {
	strokeCountThreshold: number;
	timeoutMs: number;
	consumerGroup: string;
}

interface RoomBatchData {
	strokeCount: number;
	strokesData: string[];
	isTimedOut: boolean;
}

class DrawingPersistenceManager {
	private readonly streamManager: RedisStreamManager;
	private readonly redis: Redis;
	private readonly config: BatchProcessingConfig;
	private isInitialized = false;
	private readonly redisConfig = {
		host: 'localhost',
		port: 6380,
		retryDelayOnFailover: 100,
		maxRetriesPerRequest: 3,
	};

	constructor(config?: Partial<BatchProcessingConfig>) {
		this.streamManager = new RedisStreamManager();
		this.redis = new Redis(this.redisConfig);

		this.config = {
			strokeCountThreshold: 10,
			timeoutMs: 10 * 1000,
			consumerGroup: 'processingServersTest',
			...config,
		};

		this.initialize();
	}

	public async initialize(): Promise<void> {
		if (this.isInitialized) return;

		try {
			await this.initializeRedisStreams();
			console.log('Redis streams initialized');

			this.streamManager.consumeFromGroup(
				REDIS_STREAM_EVENTS.DRAWING_EVENT,
				this.config.consumerGroup,
				this.handleDrawingEvent.bind(this)
				// { timeoutHandler: this.processTimedOutRooms.bind(this) }
			);

			this.isInitialized = true;
			console.log('DrawingPersistenceManager initialized successfully');
		} catch (error) {
			console.error('Failed to initialize DrawingPersistenceManager:', error);
			throw error;
		}
	}

	// ========== Event Handlers ==========

	private async handleDrawingEvent(
		data: any,
		messageId: string,
		streamName: string
	): Promise<void> {
		try {
			const { messageData } = data.data;
			const roomId = messageData.roomId;

			// await this.redis.flushall();

			await this.addStrokeToRoom(roomId, messageData);
		} catch (error) {
			console.error('Error handling drawing event:', error);
			throw error;
		}
	}

	// private async processTimedOutRooms(): Promise<void> {
	// 	const activeRoomIds = await this.getActiveRoomIds();
	// 	console.log('Checking timed out rooms:', activeRoomIds);

	// 	const timeoutPromises = activeRoomIds.map((roomId) =>
	// 		this.checkAndProcessTimedOutRoom(roomId)
	// 	);

	// 	await Promise.all(timeoutPromises);
	// }

	// ========== Room Management ==========
	private async addStrokeToRoom(
		roomId: string,
		strokeData: any
	): Promise<void> {
		try {
			const roomKey = this.getRoomKey(roomId);
			const strokesKey = this.getRoomStrokesKey(roomId);
			const now = Date.now();
			console.log('added to the room');

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
			if (shouldTriggerBatchWrite) {
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
					return; // Another process is handling this room
				}

				// Process batch write asynchronously but handle lock release properly
				this.processBatchWriteWithLock(roomId, lockKey, lockValue).catch(
					(error: any) => {
						console.error(`Batch write failed for room ${roomId}:`, error);
					}
				);
			}
		} catch (error) {
			console.error(`Error adding stroke to room ${roomId}:`, error);
			throw error;
		}
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
		const luaScript = `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`;

		try {
			await this.redis.eval(luaScript, 1, lockKey, lockValue);
		} catch (error) {
			console.error('Failed to release lock:', error);
		}
	}

	// private async checkAndProcessTimedOutRoom(roomId: string): Promise<void> {
	// 	const roomKey = this.getRoomKey(roomId);
	// 	const lastUpdated = await this.getRoomLastUpdated(roomId);

	// 	if (!lastUpdated) return;

	// 	const timeSinceUpdate = Date.now() - lastUpdated;
	// 	if (timeSinceUpdate > this.config.timeoutMs) {
	// 		const hasUnprocessedStrokes = await this.hasUnprocessedStrokes(roomId);
	// 		if (hasUnprocessedStrokes) {
	// 			console.log(`Processing timed out room: ${roomId}`);
	// 			await this.markRoomAsTimedOut(roomId);
	// 			// await this.processBatchWrite(roomId);
	// 		}
	// 	}
	// }

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
			});
			// this.cleanupProcessedStrokes(roomId, strokes);

			// Handle partial failures
			const successfulInserts = result.insertedCount || 0;
			const failedInserts = strokes.length - successfulInserts;

			if (failedInserts > 0) {
				console.warn(
					`${failedInserts} out of ${strokes.length} inserts failed for room ${roomId}`
				);

				// Clean up only successful strokes (if you can identify them)
				await this.cleanupSuccessfulStrokes(roomId, strokes, result);
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
		originalStrokes: RoomDataBase[],
		bulkWriteResult: any
	): Promise<void> {
		if (originalStrokes.every((stroke) => stroke.packageId)) {
			// if theres packageId in each element is truthy then proceed
			const insertedIds = await this.getInsertedStrokeIds(originalStrokes);

			const successfulStrokes = originalStrokes.filter((stroke) =>
				insertedIds.includes(stroke.packageId)
			);
			console.log(
				'insertedIds',
				insertedIds,
				'originalStrokes',
				originalStrokes,
				'successfulStrokes',
				successfulStrokes
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
			console.log('insertedDocs', insertedDocs);

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

	// ========== Redis Helper Methods ==========

	private getRoomKey(roomId: string): string {
		return `room:${roomId}`;
	}

	private getRoomStrokesKey(roomId: string): string {
		return `room:${roomId}:strokes`;
	}

	private async setRoomMetadata(
		roomKey: string,
		metadata: RoomMetadata
	): Promise<void> {
		await this.redis.hset(roomKey, {
			roomId: metadata.roomId,
			strokeCount: metadata.strokeCount.toString(),
			createdAt: metadata.createdAt.toString(),
			lastUpdated: metadata.lastUpdated.toString(),
			isTimedOut: metadata.isTimedOut ? '1' : '0',
		});
	}

	private async getRoomStrokeCount(roomId: string): Promise<number> {
		const count = await this.redis.hget(this.getRoomKey(roomId), 'strokeCount');
		return parseInt(count || '0');
	}

	private async getRoomLastUpdated(roomId: string): Promise<number | null> {
		const lastUpdated = await this.redis.hget(
			this.getRoomKey(roomId),
			'lastUpdated'
		);
		return lastUpdated ? parseInt(lastUpdated) : null;
	}

	private async markRoomAsTimedOut(roomId: string): Promise<void> {
		await this.redis.hset(this.getRoomKey(roomId), 'isTimedOut', '1');
	}

	private async hasUnprocessedStrokes(roomId: string): Promise<boolean> {
		const strokes = await this.redis.smembers(this.getRoomStrokesKey(roomId));
		return strokes.length > 0;
	}

	private async getActiveRoomIds(): Promise<string[]> {
		return await this.redis.smembers('activeRooms');
	}

	private async addToActiveRooms(roomId: string): Promise<void> {
		await this.redis.sadd('activeRooms', roomId);
		await this.redis.expire('activeRooms', 3600);
	}

	private async removeFromActiveRooms(roomId: string): Promise<void> {
		await this.redis.srem('activeRooms', roomId);
	}

	// ========== Stream Initialization ==========

	private async initializeRedisStreams(): Promise<void> {
		try {
			await this.streamManager.initialize(REDIS_STREAM_EVENTS.DRAWING_EVENT);
		} catch (error) {
			console.error('Failed to initialize Redis streams:', error);
			throw error;
		}
	}
}

export default DrawingPersistenceManager;
