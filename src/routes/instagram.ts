import express from "express";
import {
  webhookVerification,
  webhookHandler,
  refreshToken,
} from "../controllers/instagram";

const router = express.Router();

// Meta webhook verification (subscription setup)
router.get("/webhook", webhookVerification);

// Meta webhook events (incoming DMs)
router.post("/webhook", webhookHandler);

// Scheduled long-lived token refresh (called by Vercel cron)
router.get("/refresh-token", refreshToken);

export default router;
