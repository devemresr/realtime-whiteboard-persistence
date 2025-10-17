import Redis from 'ioredis';
import { parseRedisFields } from '../../utils/parseRedisFields';
import { persistedMessages, StreamEvents } from '../../events/StreamEvents';
import { ackMessagesScript } from '../../scripts/redis/ackMessagesScript';

interface RedisConfig {
	host?: string;
	port?: number;
	password?: string;
	retryDelayOnFailover?: number;
	enableReadyCheck?: boolean;
	maxRetriesPerRequest?: number;
	[key: string]: any;
}

type MessageHandler = (data: any) => Promise<void>;

export interface RedisMessage {
	[key: string]: any;
}
interface RedisStreamMessage {
	messageId: string;
	message: Record<string, string>;
	strokes: any;
	packageSequenceNumber: any;
	strokeId: any;
	roomId: any;
}

interface StreamReadResult {
	streamName: string;
	messages: RedisStreamMessage[];
}

interface ConsumerOptions {
	count?: number;
	blockTime?: number;
	processingTimeout?: number;
	timeoutCheckInterval?: number;
	// timeoutHandler?: () => Promise<void>;
	timeoutHandler?: any;
}

interface MessageData {
	[key: string]: string | number | boolean;
}

interface StreamMessage {
	id: string;
	data: MessageData;
}

class RedisStreamManager {
	private redis: Redis | null = null;
	private streamName: string | null = null;
	private consumerName: string;
	private lastTimeoutCheck: number = 0;
	private streamEventEmitter: StreamEvents;

	constructor(
		streamEventEmitter: StreamEvents,
		redisInstanceForStreams: Redis
	) {
		this.streamEventEmitter = streamEventEmitter;
		this.redis = redisInstanceForStreams;
		this.consumerName = process.env.CONSUMER_NAME || `consumer-${process.pid}`;
	}

	/**
	 * Create consumer group if it doesn't exist
	 * @param {string} stream - Stream name
	 * @param {string} group - Consumer group name
	 * @param {string} startId - Starting position ('0' for beginning, '$' for new messages)
	 */
	async createConsumerGroup(stream: string, group: string, startId = '0') {
		try {
			await this.redis!.xgroup('CREATE', stream, group, startId, 'MKSTREAM');
			console.log(`Consumer group '${group}' created for stream '${stream}'`);
		} catch (error) {
			console.log(
				`Consumer group '${group}' already exists for stream '${stream}' the error: `,
				error
			);
		}
	}

	private async setupEventListener(stream: string, group: string) {
		this.streamEventEmitter.onPersistedPackages(
			async (messageIds: persistedMessages) => {
				const { redisMessageIds, roomId } = messageIds;
				if (redisMessageIds.length === 0) return;
				try {
					// since were emitting the messageIds by each room we dont need to
					const result = await this.redis!.eval(
						ackMessagesScript,
						2,
						stream,
						group,
						JSON.stringify(redisMessageIds)
					).then((i) => JSON.parse(i as string));

					const { ackResult, messageIds } = result;
					if (ackResult)
						console.log(
							'Acked: ',
							...messageIds,
							`messages successfully for the room: ${roomId}`
						);
				} catch (error) {
					console.error('Failed to ack messages:', error);
					//todo handle this error appropriately
				}
			}
		);
		return;
	}

	/**
	 * Consume messages from a stream using consumer group
	 * @param {string} stream - Stream name
	 * @param {string} group - Consumer group name
	 * @param {function} messageHandler - Function to handle each message
	 * @param {object} options - Additional options
	 */
	async consumeFromGroup(
		stream: string,
		group: string,
		messageHandler: MessageHandler,
		options: ConsumerOptions = {}
	) {
		const {
			count = 50,
			blockTime = 1 * 1000,
			processingTimeout = 3 * 1000,
			timeoutHandler,
			timeoutCheckInterval = 6 * 1000,
		} = options;

		console.log(
			`Starting consumer '${this.consumerName}' for group '${group}' on stream '${stream}'`
		);

		// One-time setup of persistent Redis XACK event listener
		// TODO: Refactor to constructor injection when implementing graceful shutdown
		await this.setupEventListener(stream, group);

		while (true) {
			if (
				timeoutHandler &&
				Date.now() - this.lastTimeoutCheck > timeoutCheckInterval
			) {
				console.log('calling timeoutHandler');
				await timeoutHandler();
				this.lastTimeoutCheck = Date.now();
			}

			const rawMessages: any = await this.redis!.xreadgroup(
				'GROUP',
				group,
				this.consumerName,
				'COUNT',
				count,
				'BLOCK',
				blockTime,
				'STREAMS',
				stream,
				'>'
			);

			if (rawMessages) console.log('rawMessages', rawMessages);

			const messages: StreamReadResult[] | null = rawMessages
				? (rawMessages as [string, [string, string[]][]][]).map(
						([streamName, msgs]) => {
							return {
								streamName,
								messages: msgs.map(([redisMessageId, fields]) => {
									return { redisMessageId, ...parseRedisFields(fields) };
								}),
							};
						}
					)
				: null;

			if (messages) console.log('messages', messages);

			try {
				const messagesByRoom = new Map<string, Array<any>>();
				if (messages && messages.length > 0) {
					for (const item of messages) {
						for (const message of item.messages) {
							const { roomId } = message;
							if (!messagesByRoom.has(roomId)) {
								messagesByRoom.set(roomId, []);
							}
							messagesByRoom.get(roomId)!.push(message);
						}
					}
				}
				for (const [roomId, roomMessages] of messagesByRoom) {
					await messageHandler(roomMessages);
				}

				// todo add Read pending messages first (messages that were delivered but not acknowledged)
			} catch (error) {
				console.error('❌ Error in consumer loop:', error);
				await new Promise((resolve) => setTimeout(resolve, 5000)); // Wait before retrying
			}
		}
	}

	/**
	 * Read messages from the stream
	 * @param start - Starting ID (e.g., '0', '-', or specific ID)
	 * @param end - Ending ID (e.g., '+', or specific ID)
	 * @param count - Maximum number of messages to read
	 */
	public async readMessages(
		start: string = '0',
		end: string = '+',
		count: number = 100
	): Promise<StreamMessage[]> {
		if (!this.redis) {
			throw new Error('Redis instance is undefined');
		}

		try {
			const messages = await this.redis.xrange(
				this.streamName!,
				start,
				end,
				'COUNT',
				count
			);
			console.log('READ MESSAGES', messages);

			return messages.map(([id, fields]) => ({
				id,
				data: this.arrayToObject(fields),
			}));
		} catch (error) {
			throw new Error(
				`Failed to read messages from stream: ${(error as Error).message}`
			);
		}
	}

	/**
	 * Get stream information
	 */
	public async getStreamInfo(): Promise<any> {
		if (!this.redis) {
			throw new Error('Redis instance is undefined');
		}

		try {
			return await this.redis.xinfo('STREAM', this.streamName!);
		} catch (error) {
			throw new Error(`Failed to get stream info: ${(error as Error).message}`);
		}
	}

	/**
	 * Get stream length
	 */
	public async getStreamLength(): Promise<number> {
		if (!this.redis) {
			throw new Error('Redis instance is undefined');
		}

		try {
			return await this.redis.xlen(this.streamName!);
		} catch (error) {
			throw new Error(
				`Failed to get stream length: ${(error as Error).message}`
			);
		}
	}

	/**
	 * Helper method to convert flat array to object
	 */
	private arrayToObject(arr: string[]): MessageData {
		const obj: MessageData = {};
		for (let i = 0; i < arr.length; i += 2) {
			obj[arr[i]] = arr[i + 1];
		}
		return obj;
	}

	/**
	 * Close Redis connection
	 */
	public async disconnect(): Promise<void> {
		if (this.redis) {
			await this.redis.quit();
			console.log('Redis connection closed');
		}
	}
}

export default RedisStreamManager;
