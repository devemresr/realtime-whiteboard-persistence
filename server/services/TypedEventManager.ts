import EventEmitter from 'eventemitter3';

/**
 * TypedEventManager is a base class for type-safe event emitters.
 *
 * T: a mapping from event names to argument tuples
 * K: the specific event key
 * listener: a function that accepts the arguments defined in T[K]
 *
 * Usage ensures that event names and listener arguments match at compile time.
 */
abstract class TypedEventManager<
	T extends { [K in keyof T]: any[] },
> extends EventEmitter {
	public onEvent<K extends keyof T>(
		event: K,
		listener: (...args: T[K]) => void
	) {
		this.on(event as string, listener);
	}

	public emitEvent<K extends keyof T>(event: K, ...args: T[K]) {
		this.emit(event as string, ...args);
	}
}

export { TypedEventManager };
