import { AutoTimeoutEventEmitter } from './AutoTimeoutEventEmitter';
interface StreamEventMap {
	persistedPackages: [persistedMessages];
	streamError: [error: Error];
	processingComplete: [batchId: string, count: number];
}

export type persistedMessages = {
	redisMessageIds: string[];
	roomId: string;
};

// stream spesific event emitter usage
class StreamEvents extends AutoTimeoutEventEmitter<StreamEventMap> {
	constructor(instanceKey: string, idleTimeout: number) {
		super(instanceKey, idleTimeout);
	}

	// Stream-specific methods with proper persistent event handling
	onPersistedPackages(callback: (persistedMessage: persistedMessages) => void) {
		this.onPersistentEvent('persistedPackages', callback);
	}

	// Generic timeout method for any event
	onEventWithTimeout<K extends keyof StreamEventMap>(
		event: K,
		callback: (...args: StreamEventMap[K]) => void
	) {
		this.onEvent(event, callback);
	}

	emitPersistedPackages(persistedMessage: persistedMessages) {
		this.emitEvent('persistedPackages', persistedMessage);
	}
}

export { StreamEvents };
