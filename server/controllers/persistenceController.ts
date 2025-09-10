import Redis from 'ioredis';
import RedisStreamManager from '../services/RedisStreamManager';
import { REDIS_STREAM_EVENTS } from '../shared/constants/socketIoConstants';
import RoomData, { RoomDataBase } from '../schemas/Strokes';

class PersistenceController {
	private streamManager: RedisStreamManager;
	private initialized: boolean = false;
	private batchthreshold: number = 10;
	private processingRooms: Set<string> = new Set();

	private redis!: Redis;
	private redisConfig = {
		host: 'localhost',
		port: 6380,
		retryDelayOnFailover: 100,
		maxRetriesPerRequest: 3,
	};

	constructor() {
		this.streamManager = new RedisStreamManager();
		this.processMessages = this.processMessages.bind(this);
		this.addRoomData = this.addRoomData.bind(this);
		this.updateRoomData = this.updateRoomData.bind(this);
		this.bulkInsertRoomData = this.bulkInsertRoomData.bind(this);
		this.batchWrite = this.batchWrite.bind(this);
		this.initalize();
	}

	// todo this method has race conditions on a later release we're going to migrate redis based locks also we're going to add sharding
	private async batchWrite(roomId: string) {
		// Check if this room is already being processed
		if (this.processingRooms.has(roomId)) {
			console.log(`Room ${roomId} is already being processed, skipping...`);
			return;
		}

		try {
			// Mark room as being processed
			this.processingRooms.add(roomId);

			const roomKey = `room:${roomId}`;

			// Get current count and strokes atomically
			const multi = this.redis.multi();
			multi.hget(roomKey, 'count');
			multi.smembers(`${roomKey}:strokes`);

			const results = await multi.exec();

			if (!results || results.length < 2) {
				console.log('Failed to get room data');
				return;
			}

			const [countResult, strokesResult] = results;
			const count = countResult[1] as string;
			const strokesData = strokesResult[1] as string[];
			// Double-check threshold (data might have changed)

			if (
				!count ||
				parseInt(count) < this.batchthreshold ||
				strokesData.length === 0
			) {
				console.log(`Room ${roomId} no longer meets batch threshold`);
				return;
			}

			console.log(
				`Processing batch write for room ${roomId} with ${strokesData.length} strokes`
			);

			// Parse the JSON strings back to objects
			const parsedStrokes = strokesData
				.map((strokeJson) => {
					try {
						return JSON.parse(strokeJson);
					} catch (parseError) {
						console.error(
							'Failed to parse stroke data:',
							strokeJson,
							parseError
						);
						return null;
					}
				})
				.filter((stroke) => stroke !== null);

			if (parsedStrokes.length === 0) {
				console.log('No valid strokes to insert');
				await this.redis.srem('activeRooms', roomId);
				return;
			}

			// Perform bulk insert to MongoDB
			await this.bulkInsertRoomData(parsedStrokes);

			// Clear the processed strokes from Redis atomically
			const cleanupPipeline = this.redis.pipeline();

			// Remove the specific strokes that were processed
			strokesData.forEach((stroke) => {
				cleanupPipeline.srem(`${roomKey}:strokes`, stroke);
			});

			// Get the current count again and subtract what we processed
			cleanupPipeline.hget(roomKey, 'count');
			const currentResults = await cleanupPipeline.exec();
			if (currentResults && currentResults.length > 0) {
				const currentCount = currentResults[
					currentResults.length - 1
				][1] as string;

				const newCount = Math.max(
					0,
					parseInt(currentCount || '0') - strokesData.length
				);

				// Update the count and last processed time
				await this.redis.hset(roomKey, {
					count: newCount.toString(),
					lastProcessed: Date.now().toString(),
				});
			}

			console.log(
				`Successfully batch inserted ${parsedStrokes.length} strokes for room ${roomId}`
			);
		} catch (error) {
			console.error(`Error in batchWrite for room ${roomId}:`, error);
			throw error;
		} finally {
			// Always remove from processing set
			this.processingRooms.delete(roomId);
		}
	}

	public async initalize() {
		if (this.initialized) return;
		try {
			this.redis = new Redis(this.redisConfig);

			await this.initializeStreams();
			console.log('Streams initialized');

			this.streamManager.consumeFromGroup(
				//since its a loop
				REDIS_STREAM_EVENTS.DRAWING_EVENT,
				'processingServersTest',
				this.processMessages
			);

			this.initialized = true;
		} catch (error) {
			console.error('Initialization error:', error);
			throw error; // important: reject the promise
		}
	}

	private async initializeStreams() {
		try {
			await this.streamManager.initialize(REDIS_STREAM_EVENTS.DRAWING_EVENT);
		} catch (error) {
			console.error('Failed to initialize streams:', error);
		}
	}

	private async bulkInsertRoomData(roomDataArray: RoomDataBase[]) {
		const operations = roomDataArray.map((roomData: RoomDataBase) => ({
			insertOne: {
				document: roomData,
			},
		}));

		try {
			const result = await RoomData.bulkWrite(operations);
			console.log('Bulk insert result:', result);
			return result;
		} catch (error) {
			console.error('Bulk insert error:', error);
			throw error;
		}
	}

	private async updateRoomData(roomId: string, data: any) {
		try {
			// Increment counter and store data atomically
			const pipeline = this.redis.pipeline();
			const roomKey = `room:${roomId}`;
			pipeline.hincrby(roomKey, 'count', 1);
			pipeline.hset(roomKey, 'lastUpdated', Date.now());
			pipeline.sadd(`${roomKey}:strokes`, JSON.stringify(data));
			await pipeline.exec();
		} catch (error) {
			console.log('error while updating roomdata:', error);
		}
	}

	private async getRoomStats(roomId: string) {
		return await this.redis.hgetall(`room:${roomId}`);
	}

	private async addRoomData(roomId: string, data: any) {
		try {
			const roomKey = `room:${roomId}`;
			const exists = await this.redis.exists(roomKey);

			if (!exists) {
				const initialData = {
					roomId: roomId,
					count: '0',
					createdAt: Date.now().toString(),
					lastUpdated: Date.now().toString(),
				};

				await this.redis.hset(roomKey, initialData);
				await this.redis.sadd('activeRooms', roomId);
				await this.redis.expire('activeRooms', 3600);
				console.log(`Created new room: ${roomId}`);
			}

			// Always update room data atomically
			const multi = this.redis.multi();
			multi.hincrby(roomKey, 'count', 1);
			multi.hset(roomKey, 'lastUpdated', Date.now());
			multi.sadd(`${roomKey}:strokes`, JSON.stringify(data));
			await multi.exec();
		} catch (error) {
			console.error(`Error adding room data for ${roomId}:`, error);
			throw error;
		}
	}

	private async getRoomsCount(roomId: string) {
		return await this.redis.hget(`room:${roomId}`, 'count');
	}

	private async processMessages(
		data: any,
		messageId: string,
		streamName: string
	) {
		const { messageData } = data.data;
		const roomId = messageData.roomId;
		await this.addRoomData(roomId, messageData);

		// Check if this room needs batch processing (non-blocking check)
		const count = await this.getRoomsCount(roomId);
		if (count && parseInt(count) >= this.batchthreshold) {
			// Process batch asynchronously without blocking message processing
			this.batchWrite(roomId).catch((error) => {
				console.error(`Batch write failed for room ${roomId}:`, error);
				// todo add the room to a retry queue here
			});
		}
	}
}
export default PersistenceController;
