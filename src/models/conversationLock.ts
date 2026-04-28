import mongoose, { Document, Schema } from "mongoose";

export interface IConversationLock extends Document {
  key: string;
  createdAt: Date;
}

const conversationLockSchema = new Schema<IConversationLock>(
  {
    key: { type: String, required: true, unique: true },
    createdAt: { type: Date, default: Date.now, expires: 15 },
  },
  { versionKey: false }
);

export default mongoose.model<IConversationLock>(
  "ConversationLock",
  conversationLockSchema
);
