import { TypedEventManager } from './TypedEventManager';

/**
 * AutoTimeoutEventEmitter - Self-Cleaning Event Manager
 *
 * Automatically removes inactive listeners after timeout to prevent memory leaks.
 *
 * IMPORTANT: Listeners added with onEvent() will STOP working after timeout - this is intentional!
 * - Use onEvent() for temporary listeners (auto-cleanup after idle timeout)
 * - Use onPersistentEvent() for permanent listeners (never removed)
 * - Any activity (onEvent/emitEvent) resets the timeout timer
 */
class AutoTimeoutEventEmitter<
	T extends { [K in keyof T]: any[] },
> extends TypedEventManager<T> {
	private idleTimer?: NodeJS.Timeout;
	private instanceKey: string;
	private persistentListeners = new Map<
		string,
		Set<(...args: any[]) => void>
	>();
	private ownListeners = new Map<string, Set<(...args: any[]) => void>>();

	constructor(
		instanceKey: string,
		private idleTimeout: number
	) {
		super();
		this.instanceKey = instanceKey;
	}

	public onEvent<K extends keyof T>(
		event: K,
		listener: (...args: T[K]) => void
	) {
		super.onEvent(event, listener);

		const eventName = event as string;
		if (!this.ownListeners.has(eventName)) {
			this.ownListeners.set(eventName, new Set());
		}
		this.ownListeners.get(eventName)!.add(listener);

		this.resetIdleTimer(this.getInstanceKey());
	}

	// Events that persist beyond timeouts and must be manually removed
	public onPersistentEvent<K extends keyof T>(
		event: K,
		listener: (...args: T[K]) => void
	) {
		super.onEvent(event, listener);

		// Track persistent listeners by event and function for cleanup
		const eventName = event as string;
		if (!this.persistentListeners.has(eventName)) {
			this.persistentListeners.set(eventName, new Set());
		}
		this.persistentListeners.get(eventName)!.add(listener);
	}

	public emitEvent<K extends keyof T>(event: K, ...args: T[K]) {
		super.emitEvent(event, ...args);

		this.resetIdleTimer(this.getInstanceKey());
	}

	private resetIdleTimer(instanceKey: string) {
		if (this.idleTimer) clearTimeout(this.idleTimer);

		this.idleTimer = setTimeout(() => {
			this.handleShutdown();
		}, this.idleTimeout);
	}

	private handleShutdown() {
		console.log('handling event cleanup');

		for (const [eventName, listeners] of this.ownListeners) {
			const listenersToRemove = [];

			// Identify listeners to remove
			for (const listener of listeners) {
				if (!this.persistentListeners.get(eventName)?.has(listener)) {
					listenersToRemove.push(listener);
				}
			}

			// Remove timed-out event listeners
			for (const listener of listenersToRemove) {
				this.removeEventListener(eventName as keyof T, listener);
				this.ownListeners.get(eventName)?.delete(listener);
			}

			// Clean up empty event entries
			if (this.ownListeners.get(eventName)?.size === 0) {
				this.ownListeners.delete(eventName);
			}
		}
	}

	getInstanceKey(): string {
		return this.instanceKey;
	}
	// todo add a cleanup func for persistentEventlisteners
}

export { AutoTimeoutEventEmitter };
