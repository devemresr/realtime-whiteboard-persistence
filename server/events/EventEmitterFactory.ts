import { AutoTimeoutEventEmitter } from './AutoTimeoutEventEmitter';
import { StreamEvents } from './StreamEvents';

class EventEmitterFactory {
	private streamInstances = new Map<string, StreamEvents>();
	private genericInstances = new Map<string, AutoTimeoutEventEmitter<any>>();

	createOrGetStreamEvents(
		instanceKey: string,
		idleTimeout: number
	): StreamEvents {
		if (!this.streamInstances.has(instanceKey)) {
			this.streamInstances.set(
				instanceKey,
				new StreamEvents(instanceKey, idleTimeout)
			);
		}

		return this.streamInstances.get(instanceKey)!;
	}

	createOrGetGenericEventEmitter<T extends { [K in keyof T]: any[] }>(
		instanceKey: string,
		idleTimeout: number
	): AutoTimeoutEventEmitter<T> {
		if (!this.genericInstances.has(instanceKey)) {
			this.genericInstances.set(
				instanceKey,
				new AutoTimeoutEventEmitter<T>(instanceKey, idleTimeout)
			);
		}
		return this.genericInstances.get(instanceKey)!;
	}
}

export { EventEmitterFactory };
