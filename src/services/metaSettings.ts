import httpStatus from "http-status";
import MetaSettings, { IMetaSettings } from "../models/metaSettings";
import ApiError from "../utils/ApiError";

/**
 * Get the single Meta settings document (there's only ever one).
 */
const getSettings = async (): Promise<IMetaSettings | null> => {
  return MetaSettings.findOne().lean() as any;
};

/**
 * Get settings formatted for the frontend (masks the access token).
 */
const getSettingsForClient = async () => {
  const settings = await getSettings();
  if (!settings) {
    return {
      configured: false,
      connected: false,
      accessToken: null,
      verifyToken: null,
      instagramPageId: null,
      lastTestedAt: null,
    };
  }

  return {
    configured: true,
    connected: settings.connected,
    accessToken: maskToken(settings.accessToken),
    verifyToken: settings.verifyToken,
    instagramPageId: settings.instagramPageId,
    lastTestedAt: settings.lastTestedAt,
  };
};

/**
 * Save (create or update) Meta credentials.
 */
const saveSettings = async (data: {
  accessToken: string;
  verifyToken: string;
  instagramPageId?: string;
}) => {
  const existing = await MetaSettings.findOne();

  // A freshly-exchanged long-lived token is valid for ~60 days. Record an
  // estimated expiry so the cron knows when to refresh (the real value gets
  // set precisely on the first refresh from Instagram's `expires_in`).
  const SIXTY_DAYS_MS = 60 * 24 * 60 * 60 * 1000;
  const estimatedExpiry = new Date(Date.now() + SIXTY_DAYS_MS);

  if (existing) {
    existing.accessToken = data.accessToken;
    existing.verifyToken = data.verifyToken;
    if (data.instagramPageId) {
      existing.instagramPageId = data.instagramPageId;
    }
    existing.connected = false;
    existing.lastTestedAt = null;
    existing.tokenExpiresAt = estimatedExpiry;
    await existing.save();
    return existing;
  }

  return MetaSettings.create({
    accessToken: data.accessToken,
    verifyToken: data.verifyToken,
    instagramPageId: data.instagramPageId ?? null,
    tokenExpiresAt: estimatedExpiry,
  });
};

/**
 * Delete/disconnect Meta settings.
 */
const deleteSettings = async () => {
  const result = await MetaSettings.deleteMany({});
  return result.deletedCount > 0;
};

/**
 * Test connection by calling Instagram Graph API /me endpoint.
 */
const testConnection = async () => {
  const settings = await MetaSettings.findOne();
  if (!settings) {
    throw new ApiError(
      httpStatus.BAD_REQUEST,
      "Meta credentials not configured"
    );
  }

  const res = await fetch(
    `https://graph.instagram.com/v18.0/me?fields=id,name,username&access_token=${settings.accessToken}`
  );

  if (!res.ok) {
    const body = await res.text();
    settings.connected = false;
    await settings.save();
    throw new ApiError(
      httpStatus.BAD_GATEWAY,
      `Instagram API error: ${body}`
    );
  }

  const data = (await res.json()) as { id?: string; name?: string; username?: string };

  settings.connected = true;
  settings.instagramPageId = data.id ?? null;
  settings.lastTestedAt = new Date();
  await settings.save();

  return {
    connected: true,
    instagramPageId: data.id,
    name: data.name,
    username: data.username,
  };
};

/**
 * Refresh the long-lived Instagram token for another 60 days.
 *
 * Instagram (graph.instagram.com) lets you refresh a long-lived token BEFORE it
 * expires — the token must still be valid and at least 24 hours old. This is
 * meant to be called on a schedule (see the Vercel cron -> /api/instagram/refresh-token).
 *
 * Skips the network call when the current token is still more than 10 days from
 * expiry, so it's safe to run daily.
 */
const refreshAccessToken = async (): Promise<{
  refreshed: boolean;
  reason?: string;
  tokenExpiresAt?: Date | null;
}> => {
  const settings = await MetaSettings.findOne();
  if (!settings || !settings.accessToken) {
    return { refreshed: false, reason: "not_configured" };
  }

  // Not due yet — only refresh when within 10 days of expiry (or expiry unknown).
  if (settings.tokenExpiresAt) {
    const msUntilExpiry = settings.tokenExpiresAt.getTime() - Date.now();
    const TEN_DAYS_MS = 10 * 24 * 60 * 60 * 1000;
    if (msUntilExpiry > TEN_DAYS_MS) {
      return {
        refreshed: false,
        reason: "not_due",
        tokenExpiresAt: settings.tokenExpiresAt,
      };
    }
  }

  const res = await fetch(
    `https://graph.instagram.com/refresh_access_token?grant_type=ig_refresh_token&access_token=${settings.accessToken}`
  );

  if (!res.ok) {
    const body = await res.text();
    settings.connected = false;
    await settings.save();
    throw new ApiError(
      httpStatus.BAD_GATEWAY,
      `Instagram token refresh failed: ${body}`
    );
  }

  const data = (await res.json()) as {
    access_token: string;
    token_type?: string;
    expires_in?: number;
  };

  settings.accessToken = data.access_token;
  settings.connected = true;
  if (data.expires_in) {
    settings.tokenExpiresAt = new Date(Date.now() + data.expires_in * 1000);
  }
  await settings.save();

  return { refreshed: true, tokenExpiresAt: settings.tokenExpiresAt };
};

/**
 * Get the active access token (used by the webhook service).
 * Sourced from the DB, which is the single writable source the refresh loop updates.
 */
const getAccessToken = async (): Promise<string | null> => {
  const settings = await MetaSettings.findOne().select("accessToken").lean();
  return settings?.accessToken ?? null;
};

/**
 * Get the verify token (used by webhook verification).
 * Sourced from the DB.
 */
const getVerifyToken = async (): Promise<string | null> => {
  const settings = await MetaSettings.findOne().select("verifyToken").lean();
  return settings?.verifyToken ?? null;
};

const maskToken = (token: string) => {
  if (token.length <= 8) return "****";
  return token.slice(0, 4) + "****" + token.slice(-4);
};

export default {
  getSettings,
  getSettingsForClient,
  saveSettings,
  deleteSettings,
  testConnection,
  refreshAccessToken,
  getAccessToken,
  getVerifyToken,
};
