import crypto from 'crypto';

function stableHash(input: string) {
	const hash = crypto
		.createHash('sha256')
		.update(input.toString())
		.digest('hex');

	// Convert hex to integer
	let result = 0;
	for (let i = 0; i < 8; i++) {
		result = result * 16 + parseInt(hash[i], 16);
	}

	return Math.abs(result);
}

export default stableHash;
