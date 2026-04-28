import mongoose, { Document, Schema } from "mongoose";

export interface IBufferedMessage {
  text: string;
  mid?: string;
  receivedAt: Date;
}

export interface IMessageBuffer extends Document {
  key: string;
  messages: IBufferedMessage[];
  lastMessageAt: Date;
  createdAt: Date;
}

const bufferedMessageSchema = new Schema<IBufferedMessage>(
  {
    text: { type: String, required: true },
    mid: { type: String },
    receivedAt: { type: Date, default: Date.now },
  },
  { _id: false }
);

const messageBufferSchema = new Schema<IMessageBuffer>(
  {
    key: { type: String, required: true, unique: true },
    messages: { type: [bufferedMessageSchema], default: [] },
    lastMessageAt: { type: Date, default: Date.now },
    createdAt: { type: Date, default: Date.now, expires: 300 },
  },
  { versionKey: false }
);

export default mongoose.model<IMessageBuffer>(
  "MessageBuffer",
  messageBufferSchema
);
