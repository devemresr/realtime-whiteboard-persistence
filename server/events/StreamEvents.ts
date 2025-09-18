import { AutoTimeoutEventEmitter } from './AutoTimeoutEventEmitter';
interface StreamEventMap {
	persistedPackages: [persistedMessageIds: string[]];
	streamError: [error: Error];
	processingComplete: [batchId: string, count: number];
}

class StreamEvents extends AutoTimeoutEventEmitter<StreamEventMap> {
	constructor(instanceKey: string, idleTimeout: number) {
		super(instanceKey, idleTimeout);
	}

	// Stream-specific methods
	onPersistedPackages(callback: (messageIds: string[]) => void) {
		this.onEvent('persistedPackages', callback);
	}

	emitPersistedPackages(messageIds: string[]) {
		this.emitEvent('persistedPackages', messageIds);
	}
}

export { StreamEvents };
