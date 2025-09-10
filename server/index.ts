import express from 'express';
import mongoose from 'mongoose';
import { createServer } from 'node:http';
import credentials from './config/credantials';
import corsOptions from './config/corsOptions';
import cors from 'cors';
import cookieParser from 'cookie-parser';
import path from 'path';
import * as dotenv from 'dotenv';
import PersistenceController from './controllers/persistenceController';
const __dirname = path.dirname(process.argv[1]);
const envPath = path.resolve(__dirname, '../.env');
dotenv.config({ path: envPath });

const PORT = process.argv[2] || 3000;
const app = express();
const httpServer = createServer(app);
app.use(credentials);
app.set('trust proxy', 1);
app.use(cors(corsOptions));
app.use(cookieParser());
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

const persistenceController = new PersistenceController();
persistenceController
	.initalize()
	.then(() => console.log('persistence server initialized'))
	.catch((err) => {
		console.log('error at initializing the persistence server:', err);
	});

const mongoUri = process.env.MONGODB_URI;
if (!mongoUri) {
	console.error('MONGODB_URI is not defined in environment variables.');
	process.exit(1);
}

mongoose
	.connect(mongoUri)
	.then(() =>
		httpServer.listen(PORT, () => {
			console.log('connected at port: ', PORT);
		})
	)
	.catch((err) => {
		console.log('error at connecting: ', err);
	});

export default app;
