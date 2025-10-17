import Redis from 'ioredis';
import RedisStreamManager from '../services/redis/RedisStreamManager';
import { REDIS_STREAMS } from '../constants/RedisConstants';
import RoomData, { RoomDataBase } from '../schemas/Strokes';
import { addToTheRoomAndCheckThreshold } from '../scripts/redis/addToTheRoomAndCheckThresholdScript';
import getInflightRoomDataScript from '../scripts/redis/getInflightRoomDataScript';
import cleanupProcessedStrokesScript from '../scripts/redis/cleanupProcessedStrokesScript';
import stableHash from '../utils/stableHash';
import checkAndDetermineTimeout from '../scripts/redis/checkAndDetermineTimeoutScript';
import HeartbeatService from '../services/heartbeat/HeartbeatService';
import { StreamEvents } from '../events/StreamEvents';
import { json } from 'express';

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
	private serverId: string;
	private redisStreamManager: RedisStreamManager;
	private activeRoomsKey: string = process.env.ACTIVE_ROOMS_KEY!;

	constructor(
		PORT: string,
		streamEventEmitter: StreamEvents,
		config: BatchProcessingInput,
		redisStreamManager: RedisStreamManager,
		heartBeatService: HeartbeatService,
		redis: Redis
	) {
		this.redis = redis;
		this.streamEventEmitter = streamEventEmitter;
		this.redisStreamManager = redisStreamManager;
		this.PORT = PORT;
		// Server ID priority: process.env.PORT (if set) → config.port (from CLI flag)
		// This allows multiple dev instances sharing the same .env to have unique IDs
		// based on their --port flag. Production should use explicit SERVER_ID env vars.
		const derivedServerId = process.env.PORT
			? `server-${process.env.PORT}`
			: undefined;
		this.serverId = derivedServerId ?? `server-${this.PORT}`;
		this.heartBeatService = heartBeatService;

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
				REDIS_STREAMS.DRAWING_EVENTS,
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

			const roomId = data[0].roomId;
			await this.addStrokeToRoom(roomId, data);
		} catch (error) {
			console.error('Error handling drawing event:', error);
			throw error;
		}
	}

	// ========== Room Management ==========
	private async addStrokeToRoom(roomId: string, data: any): Promise<void> {
		try {
			const inflightHashKey = this.getInflightHashKey(roomId);
			const inflightMetadataHashKey = this.getInflightMetadataHashKey(roomId);
			const now = Date.now();

			// This call also set the ActiveRooms
			const shouldTriggerBatchWriteResult = (await this.redis.eval(
				addToTheRoomAndCheckThreshold,
				2, // number of KEYS
				inflightHashKey,
				inflightMetadataHashKey,
				roomId,
				JSON.stringify(data),
				now.toString(),
				this.config.strokeCountThreshold,
				this.activeRoomsKey
			)) as [number, number];

			const shouldTriggerBatchWrite = shouldTriggerBatchWriteResult[1];
			const strokeCount = shouldTriggerBatchWriteResult[0];
			console.log(
				'added to the room: ',
				roomId,
				'current room stroke count: ',
				strokeCount
			);

			if (shouldTriggerBatchWrite) {
				const acquireLockResults = await this.acquireLock(roomId);
				if (!acquireLockResults) {
					console.log(
						' COULDNT acquire the lock another server acquired it for the room: ',
						roomId
					);
					return;
				}
				const { lockKey, lockValue } = acquireLockResults;
				await this.processBatchWriteWithLock(roomId, lockKey, lockValue);
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
			//todo this is O(N) and shouldnt be used for large scale applications. migration to consistent hash and usage of pub sub is determined to be done by the next version
			const activeRoomIds = await this.getActiveRoomIds();
			console.log('activeRoomIds', activeRoomIds);

			if (activeRoomIds.length === 0) {
				console.log('no active rooms to check');
				return;
			}

			for (let activeRoomId of activeRoomIds) {
				await this.checkAndProcessTimedOutRoom(activeRoomId);
			}
			return;
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
			console.log('activeServers', activeServers);

			const myserverIndex = activeServers.findIndex(
				(server) => server.id === this.serverId
			);
			if (myserverIndex === -1) {
				console.log('this server failed to give heartbeat');
				return;
			}

			const cycle_number = Math.floor(Date.now() / (10 * 1000));
			console.log('cycle_number', cycle_number);
			const responsibleIndex =
				stableHash(`${roomId}-${cycle_number}`) % activeServers.length;

			return responsibleIndex === myserverIndex;
		} catch (error) {
			console.error('Error determining server responsibility:', error);
			return false;
		}
	}

	private async checkAndProcessTimedOutRoom(roomId: string): Promise<void> {
		const maxRetries = 3;
		for (let attempt = 1; attempt <= maxRetries; attempt++) {
			try {
				// Watch for server topology changes
				const ACTIVE_SERVERS_KEY = process.env.ACTIVE_SERVERS_KEY!;
				const ACTIVE_SERVER_DATA = process.env.ACTIVE_SERVER_DATA!;
				await this.redis.watch(ACTIVE_SERVERS_KEY, ACTIVE_SERVER_DATA);

				// Check if we're responsible (outside transaction)
				const isResponsible = await this.isMyRoom(roomId);
				if (!isResponsible) {
					await this.redis.unwatch();
					return;
				}

				console.log('serverId', this.serverId, 'isResponsible for:', roomId);
				const inflightMetadataHashKey = this.getInflightMetadataHashKey(roomId);

				const timeoutResult = (await this.redis.eval(
					checkAndDetermineTimeout,
					1,
					inflightMetadataHashKey,
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

				// Prepare lock acquisition
				const lockKeyPrefix = process.env.SERVER_TYPE;
				const lockKey = `${lockKeyPrefix}lock:room:${roomId}`;
				const lockValue = `${process.pid}-${Date.now()}`;

				// Execute transaction with lock acquisition
				const multi = this.redis.multi();
				multi.set(lockKey, lockValue, 'EX', 30, 'NX');
				const result = await multi.exec();

				if (result === null) {
					// Server topology changed while checking responsibility and lock acquisation
					console.log('topology changed');
					continue;
				}

				// Check if we acquired the lock (first command in transaction)
				const lockAcquired = result[0][1]; // [error, result] tuple
				console.log('lockAcquired', lockAcquired);

				if (!lockAcquired) {
					console.log(
						this.serverId,
						' COULDNT acquire the lock another server acquired it for the room: ',
						roomId
					);
					return; // Another server is handling it
				}

				console.log(this.serverId, ' acquired the lock for the room: ', roomId);
				// Process batch write asynchronously
				// Batch operations are scoped to a single room to maintain data consistency
				// and simplify cleanup - both write batching and cleanup logic expect one roomId
				this.processBatchWriteWithLock(roomId, lockKey, lockValue).catch(
					(error: any) => {
						console.error(`Batch write failed for room ${roomId}:`, error);
					}
				);

				return;
			} catch (error) {
				console.error(
					`Process room ${roomId} attempt ${attempt} failed:`,
					error
				);
				await this.redis.unwatch();
				if (attempt === maxRetries + 1) {
					console.error(
						`Failed to process room ${roomId} after ${maxRetries} attempts`
					);
				}
			}
		}
	}

	private async acquireLock(roomId: string) {
		const lockKeyPrefix = process.env.SERVER_TYPE;
		const lockKey = `${lockKeyPrefix}lock:room:${roomId}`;
		const lockValue = `${process.pid}-${Date.now()}`; // unique identifier
		const acquired = await this.redis.set(
			lockKey,
			lockValue,
			'EX',
			30, // 30 second timeout for database operations
			'NX'
		);

		if (!acquired) {
			console.log(
				this.serverId,
				' COULDNT acquire the lock another server acquired it for the room: ',
				roomId
			);
			return false; // Another process is handling this room
		}

		return { lockKey, lockValue };
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

			let batchData: string[] = [];
			for (let i = 0; i < raw.length; i += 2) {
				const data = raw[i + 1];
				batchData.push(data);
			}

			const validStrokes = this.parseAndValidateStrokes(batchData);
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
	private async getRoomBatchData(roomId: string): Promise<string[]> {
		const inflightHashKey = this.getInflightHashKey(roomId);

		const res = (await this.redis.eval(
			getInflightRoomDataScript,
			1, // number of KEYS
			inflightHashKey
		)) as string[];

		return res;
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
		try {
			// Each batch processes a single room. Persisted strokes are moved to a room-specific
			// hash to be used and clear by snapshot servers
			const persistedRoomsKey = this.getPersistedRoomsKey(roomId);
			const inflightHashKey = this.getInflightHashKey(roomId);
			const inflightMetadataHashKey = this.getInflightMetadataHashKey(roomId);

			const args = [
				Date.now().toString(),
				this.activeRoomsKey,
				JSON.stringify(processedStrokes),
				roomId,
			];

			const result = (await this.redis.eval(
				cleanupProcessedStrokesScript,
				3, // Number of KEYS
				inflightHashKey,
				inflightMetadataHashKey,
				persistedRoomsKey,
				...args
			)) as [number, number, string[]];

			const [newCount, removedCount, processedStrokesRedisMessageIds] = result;

			console.log(`Cleanup results for room ${roomId}:`, {
				processedStrokes: processedStrokesRedisMessageIds.length,
				removedFromInflight: removedCount,
				newCount,
			});

			// All operations were done by roomId so one roomId is enough
			this.streamEventEmitter.emitPersistedPackages({
				redisMessageIds: processedStrokesRedisMessageIds,
				roomId,
			});
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
	private getInflightHashKey(roomId: string): string {
		return `${process.env.INFLIGHT_HASH_KEY_PREFIX}${roomId}`;
	}

	private getInflightMetadataHashKey(roomId: string): string {
		return `${process.env.INFLIGHT_HASH_METADATA_KEY_PREFIX}${roomId}`;
	}

	private getPersistedRoomsKey(roomId: string): string {
		return `${process.env.PERSISTED_ROOMS_KEY_PREFIX}${roomId}`;
	}

	private async getActiveRoomIds(): Promise<string[]> {
		return await this.redis.smembers(this.activeRoomsKey);
	}

	private async removeFromActiveRooms(roomId: string): Promise<void> {
		await this.redis.srem(this.activeRoomsKey, roomId);
	}

	private async getActiveServers() {
		return await this.heartBeatService.getActiveServers();
	}
}

export default PersistenceController;
