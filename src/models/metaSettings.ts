import mongoose, { Document, Schema } from "mongoose";

export interface IMetaSettings extends Document {
  /** Instagram page access token */
  accessToken: string;
  /** Webhook verify token */
  verifyToken: string;
  /** Instagram page/account ID (filled after a successful test) */
  instagramPageId: string | null;
  /** Whether the connection has been verified */
  connected: boolean;
  /** Timestamp of last successful connection test */
  lastTestedAt: Date | null;
  /** When the current long-lived token expires (set after a refresh) */
  tokenExpiresAt: Date | null;
}

const metaSettingsSchema = new Schema<IMetaSettings>(
  {
    accessToken: { type: String, required: true },
    verifyToken: { type: String, required: true },
    instagramPageId: { type: String, default: null },
    connected: { type: Boolean, default: false },
    lastTestedAt: { type: Date, default: null },
    tokenExpiresAt: { type: Date, default: null },
  },
  { timestamps: true }
);

export default mongoose.model<IMetaSettings>("MetaSettings", metaSettingsSchema);
