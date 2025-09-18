import mongoose, { Schema, Document } from 'mongoose';

interface Stroke {
	x: number;
	y: number;
	timestamp: number;
}

// Base interface for data operations (insert, update)
export interface RoomDataBase {
	roomId: string;
	strokes: Stroke[];
	strokeId: string;
	packageSequenceNumber: number;
	isLastPackage?: boolean;
	strokeSequenceNumber: number;
	packageId: string;
	redisMessageId: string;
}

// Document interface that extends both base and Document
export interface RoomData extends RoomDataBase, Document {}

const StrokeSchema = new Schema<Stroke>(
	{
		x: { type: Number, required: true },
		y: { type: Number, required: true },
		timestamp: { type: Number, required: true },
	},
	{ _id: false } // no separate id for subdocs
);

const RoomDataSchema = new Schema<RoomData>(
	{
		roomId: { type: String, required: true },
		strokes: { type: [StrokeSchema], required: true },
		strokeId: { type: String, required: true },
		packageSequenceNumber: { type: Number, required: true },
		isLastPackage: { type: Boolean, required: false },
		strokeSequenceNumber: { type: Number, required: true },
		packageId: { type: String, required: true },
		redisMessageId: { type: String, required: false },
	},
	{ timestamps: true }
);

export default mongoose.model<RoomData>('RoomData', RoomDataSchema);
