function generateTimeBucket(timestamp: number, interval: string = 'hour') {
	const date = new Date(timestamp);

	switch (interval) {
		case 'hour':
			return `${date.getFullYear()}-${date.getMonth() + 1}-${date.getDate()}-${date.getHours()}`;
		case 'day':
			return `${date.getFullYear()}-${date.getMonth() + 1}-${date.getDate()}`;
		case 'week':
			const weekStart = new Date(date.setDate(date.getDate() - date.getDay()));
			return `${weekStart.getFullYear()}-W${Math.ceil(weekStart.getDate() / 7)}`;
	}
}

export default generateTimeBucket;
