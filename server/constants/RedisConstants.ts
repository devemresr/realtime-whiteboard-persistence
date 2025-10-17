export const REDIS_STREAMS = {
	DRAWING_EVENTS: 'drawing:events',
	COMPLETED_DRAWING_EVENTS: 'drawing:completed',
} as const;

export const REDIS_CONSUMER_GROUPS = {
	PERSISTENCE: 'persistenceGroup',
	ONBOARDING: 'onboardingGroup',
} as const;

export const REDIS_CLIENTS = {
	MAIN: 'mainClient',
} as const;
