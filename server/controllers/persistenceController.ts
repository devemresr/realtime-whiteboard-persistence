import Redis from 'ioredis';
import RedisStreamManager from '../services/RedisStreamManager';
import { REDIS_STREAM_EVENTS } from '../shared/constants/socketIoConstants';
import RoomData, { RoomDataBase } from '../schemas/Strokes';

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

class DrawingPersistenceManager {
	private readonly streamManager: RedisStreamManager;
	private readonly redis: Redis;
	private readonly config: BatchProcessingConfig;

	private isInitialized = false;
	private roomsBeingProcessed = new Set<string>();

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

	private async handleDrawingEvent(
		data: any,
		messageId: string,
		streamName: string
	): Promise<void> {
		try {
			const { messageData } = data.data;
			const roomId = messageData.roomId;

			await this.addStrokeToRoom(roomId, messageData);

			const shouldTriggerBatch = await this.shouldTriggerBatchWrite(roomId);
			if (shouldTriggerBatch) {
				// Process batch asynchronously without blocking message processing
				this.processBatchWrite(roomId).catch((error) => {
					console.error(`Batch write failed for room ${roomId}:`, error);
					// TODO: Add room to retry queue
				});
			}
		} catch (error) {
			console.error('Error handling drawing event:', error);
			throw error;
		}
	}

	private async processTimedOutRooms(): Promise<void> {
		const activeRoomIds = await this.getActiveRoomIds();
		console.log('Checking timed out rooms:', activeRoomIds);

		const timeoutPromises = activeRoomIds.map((roomId) =>
			this.checkAndProcessTimedOutRoom(roomId)
		);

		await Promise.all(timeoutPromises);
	}

	// ========== Room Management ==========

	private async addStrokeToRoom(
		roomId: string,
		strokeData: any
	): Promise<void> {
		try {
			await this.ensureRoomExists(roomId);
			await this.incrementRoomStrokeCount(roomId, strokeData);
		} catch (error) {
			console.error(`Error adding stroke to room ${roomId}:`, error);
			throw error;
		}
	}

	private async ensureRoomExists(roomId: string): Promise<void> {
		const roomKey = this.getRoomKey(roomId);
		const roomExists = await this.redis.exists(roomKey);

		if (!roomExists) {
			const initialMetadata: RoomMetadata = {
				roomId,
				strokeCount: 0,
				createdAt: Date.now(),
				lastUpdated: Date.now(),
				isTimedOut: false,
			};

			await this.setRoomMetadata(roomKey, initialMetadata);
			await this.addToActiveRooms(roomId);
			console.log(`Created new room: ${roomId}`);
		}
	}

	private async incrementRoomStrokeCount(
		roomId: string,
		strokeData: any
	): Promise<void> {
		const roomKey = this.getRoomKey(roomId);
		const strokesKey = this.getRoomStrokesKey(roomId);

		const pipeline = this.redis.multi();
		pipeline.hincrby(roomKey, 'strokeCount', 1);
		pipeline.hset(roomKey, 'lastUpdated', Date.now());
		pipeline.sadd(strokesKey, JSON.stringify(strokeData));
		pipeline.sadd('activeRooms', roomId);
		pipeline.expire('activeRooms', 3600); // 1 hour TTL

		await pipeline.exec();
	}

	// ========== Batch Processing ==========

	private async shouldTriggerBatchWrite(roomId: string): Promise<boolean> {
		const strokeCount = await this.getRoomStrokeCount(roomId);
		return strokeCount >= this.config.strokeCountThreshold;
	}

	private async processBatchWrite(roomId: string): Promise<void> {
		// Prevent concurrent processing of the same room
		if (this.roomsBeingProcessed.has(roomId)) {
			console.log(`Room ${roomId} is already being processed, skipping...`);
			return;
		}

		try {
			this.roomsBeingProcessed.add(roomId);

			const batchData = await this.getRoomBatchData(roomId);
			if (!this.isBatchWriteNeeded(batchData)) {
				console.log(`Room ${roomId} no longer meets batch criteria`);
				return;
			}

			const validStrokes = this.parseAndValidateStrokes(batchData.strokesData);
			if (validStrokes.length === 0) {
				console.log('No valid strokes to process');
				await this.removeFromActiveRooms(roomId);
				return;
			}

			await this.persistStrokesToDatabase(validStrokes);
			await this.cleanupProcessedStrokes(roomId, batchData.strokesData);

			console.log(
				`Successfully batch processed ${validStrokes.length} strokes for room ${roomId}`
			);
		} catch (error) {
			console.error(`Error in batch processing for room ${roomId}:`, error);
			throw error;
		} finally {
			this.roomsBeingProcessed.delete(roomId);
		}
	}

	private async checkAndProcessTimedOutRoom(roomId: string): Promise<void> {
		const roomKey = this.getRoomKey(roomId);
		const lastUpdated = await this.getRoomLastUpdated(roomId);

		if (!lastUpdated) return;

		const timeSinceUpdate = Date.now() - lastUpdated;
		if (timeSinceUpdate > this.config.timeoutMs) {
			const hasUnprocessedStrokes = await this.hasUnprocessedStrokes(roomId);
			if (hasUnprocessedStrokes) {
				console.log(`Processing timed out room: ${roomId}`);
				await this.markRoomAsTimedOut(roomId);
				await this.processBatchWrite(roomId);
			}
		}
	}

	// ========== Data Retrieval Helpers ==========

	private async getRoomBatchData(roomId: string) {
		const roomKey = this.getRoomKey(roomId);
		const strokesKey = this.getRoomStrokesKey(roomId);

		const pipeline = this.redis.multi();
		pipeline.hget(roomKey, 'strokeCount');
		pipeline.smembers(strokesKey);
		pipeline.hget(roomKey, 'isTimedOut');

		const results = await pipeline.exec();
		if (!results || results.length < 3) {
			throw new Error('Failed to retrieve room batch data');
		}

		return {
			strokeCount: parseInt((results[0][1] as string) || '0'),
			strokesData: results[1][1] as string[],
			isTimedOut: results[2][1] === '1',
		};
	}

	private isBatchWriteNeeded(batchData: {
		strokeCount: number;
		strokesData: string[];
		isTimedOut: boolean;
	}): boolean {
		const hasStrokes = batchData.strokesData.length > 0;
		const meetsThreshold =
			batchData.strokeCount >= this.config.strokeCountThreshold ||
			batchData.isTimedOut;
		return hasStrokes && meetsThreshold;
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
		strokes: RoomDataBase[]
	): Promise<void> {
		const operations = strokes.map((stroke) => ({
			insertOne: { document: stroke },
		}));

		try {
			const result = await RoomData.bulkWrite(operations);
			console.log('Database bulk insert result:', result);
		} catch (error) {
			console.error('Database bulk insert failed:', error);
			throw error;
		}
	}

	private async cleanupProcessedStrokes(
		roomId: string,
		processedStrokes: string[]
	): Promise<void> {
		const roomKey = this.getRoomKey(roomId);
		const strokesKey = this.getRoomStrokesKey(roomId);

		// Remove processed strokes
		const pipeline = this.redis.pipeline();
		processedStrokes.forEach((stroke) => {
			pipeline.srem(strokesKey, stroke);
		});

		// Update room metadata
		pipeline.hget(roomKey, 'strokeCount');
		const results = await pipeline.exec();

		if (results && results.length > 0) {
			const currentCount = parseInt(
				(results[results.length - 1][1] as string) || '0'
			);
			const newCount = Math.max(0, currentCount - processedStrokes.length);

			await this.redis.hset(roomKey, {
				strokeCount: newCount.toString(),
				lastProcessed: Date.now().toString(),
				isTimedOut: '0',
			});
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
