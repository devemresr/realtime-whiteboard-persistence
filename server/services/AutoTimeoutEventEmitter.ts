import { TypedEventManager } from './TypedEventManager';

class AutoTimeoutEventEmitter<
	T extends { [K in keyof T]: any[] },
> extends TypedEventManager<T> {
	private idleTimer?: NodeJS.Timeout;
	private instanceKey: string;

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
		this.resetIdleTimer();
	}

	public emitEvent<K extends keyof T>(event: K, ...args: T[K]) {
		super.emitEvent(event, ...args);
		this.resetIdleTimer();
	}

	private resetIdleTimer() {
		if (this.idleTimer) clearTimeout(this.idleTimer);

		this.idleTimer = setTimeout(() => {
			this.handleShutdown();
		}, this.idleTimeout);
	}

	private handleShutdown() {
		this.removeAllListeners();
	}

	getInstanceKey(): string {
		return this.instanceKey;
	}
}

export { AutoTimeoutEventEmitter };
