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
import Redis from 'ioredis';
import HeartbeatService from './services/HeartbeatService';
const __dirname = path.dirname(process.argv[1]);
const envPath = path.resolve(__dirname, '../.env');
dotenv.config({ path: envPath });

const PORT = process.argv[2] || '3000';
const app = express();
const httpServer = createServer(app);
app.use(credentials);
app.set('trust proxy', 1);
app.use(cors(corsOptions));
app.use(cookieParser());
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

async function startServer() {
	try {
		console.log('port ', PORT);

		const heartbeatInstance = HeartbeatService.getInstance({ port: PORT });
		const persistenceController = new PersistenceController(PORT);
		await persistenceController.initialize();
		console.log('persistence server initialized');

		// Connect to MongoDB
		const mongoUri = process.env.MONGODB_URI;
		if (!mongoUri) {
			console.error('MONGODB_URI is not defined in environment variables.');
			process.exit(1);
		}

		await mongoose.connect(mongoUri);
		console.log('MongoDB connected');

		// Start HTTP server
		await new Promise((resolve) => {
			httpServer.listen(parseInt(PORT), () => {
				console.log('connected at port: ', PORT);
				resolve(void 0);
			});
		});

		// start heartbeat

		await heartbeatInstance.start();

		console.log('Heartbeat started - server fully operational');
	} catch (err) {
		console.log('error during server startup:', err);
		process.exit(1);
	}
}

// Graceful shutdown
process.on('SIGINT', async () => {
	try {
		console.log('Shutting down gracefully...');

		// Remove heartbeat on shutdown and quit redis
		const heartbeatInstance = HeartbeatService.getInstance({ port: PORT });
		await heartbeatInstance.stop();

		// Close HTTP server
		httpServer.close();
		console.log('Graceful shutdown complete');
	} catch (error) {
		console.error('Error during shutdown:', error);
	} finally {
		process.exit(0);
	}
});

startServer().catch((err) => {
	console.error('Failed to start server:', err);
	process.exit(1);
});

export default app;
