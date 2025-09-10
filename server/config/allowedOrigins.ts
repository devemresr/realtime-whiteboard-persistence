import path from 'path';
import * as dotenv from 'dotenv';

const __dirname = path.dirname(process.argv[1]);
const envPath = path.resolve(__dirname, '../.env');
dotenv.config({ path: envPath });

const allowedOrigins = [
	`${process.env.DEV_CLIENT_ORIGIN}`,
	'http://localhost:3001',
	'http://localhost:8080',
	'http://localhost:8080/',
];

export default allowedOrigins;
