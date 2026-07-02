import { Request, Response } from "express";
import {
  handleInstagramWebhook,
  verifyInstagramWebhook,
} from "../services/instagram";
import metaSettingsService from "../services/metaSettings";

/**
 * GET /api/instagram/webhook
 * Meta calls this to verify the webhook subscription.
 */
export const webhookVerification = async (req: Request, res: Response) => {
  console.log("[IG] http.GET /webhook (verification)", JSON.stringify(req.query));
  const result = await verifyInstagramWebhook(req.query as any);

  if (result.success) {
    console.log("[IG] ✅ webhook.verified");
    res.status(200).send(result.challenge);
  } else {
    console.warn("[IG] ❌ webhook.verify-failed");
    res.sendStatus(403);
  }
};

/**
 * POST /api/instagram/webhook
 * Meta sends incoming DM events here.
 * We respond 200 immediately, then process asynchronously.
 */
export const webhookHandler = async (req: Request, res: Response) => {
  console.log(
    "[IG] http.POST /webhook",
    JSON.stringify({
      contentType: req.headers["content-type"],
      bodyKeys: req.body ? Object.keys(req.body) : [],
      entryCount: Array.isArray(req.body?.entry) ? req.body.entry.length : 0,
    })
  );

  try {
    await handleInstagramWebhook(req.body);
  } catch (err) {
    console.error(
      "[IG] webhook.processing-error",
      err instanceof Error ? err.stack : err
    );
  }

  res.status(200).send("EVENT_RECEIVED");
};

/**
 * GET /api/instagram/refresh-token
 * Called by the Vercel cron to extend the long-lived Instagram token for
 * another 60 days before it expires. Protected by CRON_SECRET: Vercel cron
 * sends it as `Authorization: Bearer <CRON_SECRET>`.
 */
export const refreshToken = async (req: Request, res: Response) => {
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret && req.headers.authorization !== `Bearer ${cronSecret}`) {
    res.sendStatus(401);
    return;
  }

  try {
    const result = await metaSettingsService.refreshAccessToken();
    console.log("[IG] token.refresh", JSON.stringify(result));
    res.status(200).json(result);
  } catch (err) {
    console.error(
      "[IG] token.refresh-error",
      err instanceof Error ? err.message : err
    );
    res.status(502).json({
      refreshed: false,
      error: err instanceof Error ? err.message : "refresh failed",
    });
  }
};
