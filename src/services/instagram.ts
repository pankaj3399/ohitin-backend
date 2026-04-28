import { ASSISTANT_DEFAULT_MESSAGE } from "../config/assistant";
import InstagramConversation, {
  IInstagramConversation,
} from "../models/instagramConversation";
import { classifyIntentWithGemini, generateChatResponse } from "./gemini";
import {
  detectProducerBranch,
  detectSelfIdentification,
} from "./keywordRouter";
import { addUniqueTags, mapIntentToTags } from "./tagManager";
import { getFlowReply, getReplyLimitForFlow } from "./flowEngine";
import {
  buildAssistantMessage,
  buildUserMessage,
  shouldWaitForContact,
} from "./responseGenerator";
import {
  sendInstagramMessage,
  sendInstagramTypingIndicator,
} from "./instagramApi";
import metaSettingsService from "./metaSettings";
import ProcessedMessage from "../models/processedMessage";
import ConversationLock from "../models/conversationLock";
import MessageBuffer, { IBufferedMessage } from "../models/messageBuffer";


const log = (stage: string, data?: Record<string, unknown>) => {
  if (data !== undefined) {
    console.log(`[IG] ${stage}`, JSON.stringify(data));
  } else {
    console.log(`[IG] ${stage}`);
  }
};

const logErr = (stage: string, err: unknown, data?: Record<string, unknown>) => {
  const info = err instanceof Error ? { message: err.message, stack: err.stack } : { err };
  console.error(`[IG] ${stage}`, JSON.stringify({ ...info, ...(data ?? {}) }));
};

// ─── Per-user drainer lock (Mongo-backed) ────────────────────────────
// At most one Lambda per user runs the buffer drain loop at a time. The
// first message in a burst takes the lock; subsequent messages append to
// the buffer and exit. The TTL on ConversationLock auto-cleans orphaned
// locks if a Lambda crashes mid-drain.
const acquireLock = async (key: string): Promise<boolean> => {
  try {
    await ConversationLock.create({ key });
    return true;
  } catch (err: any) {
    if (err?.code === 11000) return false;
    throw err;
  }
};

const releaseLock = async (key: string): Promise<void> => {
  await ConversationLock.deleteOne({ key }).catch(() => {});
};

// ─── Webhook deduplication (Mongo-backed) ────────────────────────────
// Meta occasionally redelivers the same webhook. ProcessedMessage has a
// unique index + 10-min TTL on `mid`, so a duplicate insert throws and
// we treat it as a duplicate. Works across Vercel instances.
const isDuplicateMessage = async (mid: string | undefined): Promise<boolean> => {
  if (!mid) return false;
  try {
    await ProcessedMessage.create({ mid });
    return false;
  } catch (err: any) {
    if (err?.code === 11000) return true;
    throw err;
  }
};

// ─── Message coalescing (Mongo-backed buffer) ────────────────────────
// We optimize for fast replies (Intercom/ManyChat-style) but still
// gracefully handle burst typing.
//
// Behavior:
//   - 1 message in the wait window → reply immediately to it (fast path)
//   - 2+ messages in the wait window → combine them into a single reply
//   - Wait window is short (1s) so single-message replies feel snappy
//
// Flow per webhook:
//   1. Append message to MessageBuffer (atomic upsert).
//   2. Try to acquire ConversationLock. If another Lambda already holds
//      it, exit — our message is in the buffer for the drainer to pick up.
//   3. If we got the lock, sleep for COALESCE_WINDOW_MS, then drain.
//      Whatever's in the buffer at drain time gets one combined reply.
const COALESCE_WINDOW_MS = 1000;
const DRAIN_MAX_TOTAL_MS = 8_000;
const BURST_MAX_MESSAGES = 10;
const BURST_MAX_CHARS = 4000;

const appendToBuffer = async (
  key: string,
  text: string,
  mid: string | undefined
): Promise<void> => {
  const now = new Date();
  await MessageBuffer.updateOne(
    { key },
    {
      $push: { messages: { text, mid, receivedAt: now } },
      $set: { lastMessageAt: now },
      $setOnInsert: { key, createdAt: now },
    },
    { upsert: true }
  );
};

const readBuffer = async (key: string) => {
  return MessageBuffer.findOne({ key }).lean<{
    key: string;
    messages: IBufferedMessage[];
    lastMessageAt: Date;
  } | null>();
};

// Atomic snapshot-and-clear: returns the messages that were in the buffer
// at the moment of the call and resets the array to []. Any concurrent
// $push from a webhook will land in the now-empty array and be picked up
// by the next drain iteration — nothing is dropped.
const snapshotAndClearBuffer = async (
  key: string
): Promise<IBufferedMessage[]> => {
  const before = await MessageBuffer.findOneAndUpdate(
    { key },
    { $set: { messages: [] } }
  ).lean<{ messages: IBufferedMessage[] } | null>();
  return before?.messages ?? [];
};

const deleteBufferIfEmpty = async (key: string): Promise<void> => {
  // Only deletes the doc if no new messages have been pushed since we
  // cleared it. If new messages arrived, leaves the doc alone for the
  // next drain iteration.
  await MessageBuffer.deleteOne({ key, messages: { $size: 0 } }).catch(() => {});
};

const combineBurst = (messages: IBufferedMessage[]): string => {
  // Respect burst caps: keep most recent up to BURST_MAX_MESSAGES, then
  // truncate by char budget (oldest dropped first).
  let chosen = messages.slice(-BURST_MAX_MESSAGES);
  let total = chosen.reduce((n, m) => n + m.text.length, 0);
  while (chosen.length > 1 && total > BURST_MAX_CHARS) {
    total -= chosen[0].text.length;
    chosen = chosen.slice(1);
  }
  if (chosen.length === 1) return chosen[0].text.trim();
  return chosen.map((m) => m.text.trim()).filter(Boolean).join("\n\n");
};

// ─── Types ──────────────────────────────────────────────────────────

interface WebhookPayload {
  object: string;
  entry?: WebhookEntry[];
}

interface WebhookEntry {
  id: string;
  time: number;
  messaging?: MessagingEvent[];
}

interface MessagingEvent {
  sender?: { id: string };
  recipient?: { id: string };
  timestamp?: number;
  message?: {
    mid?: string;
    text?: string;
    is_echo?: boolean;
  };
  postback?: {
    title?: string;
    payload?: string;
  };
  read?: any;
}

// ─── Contact regex (same as chatbot) ────────────────────────────────

const emailRegex = /\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/i;
const phoneRegex =
  /(?:(?:\+?\d{1,3}[\s-]?)?(?:\(?\d{3}\)?[\s-]?)\d{3}[\s-]?\d{4})/;

const extractContactData = (message: string) => ({
  email: message.match(emailRegex)?.[0],
  phone: message.match(phoneRegex)?.[0],
});

// ─── Intent resolver (reused from chatbot) ──────────────────────────

const resolveIntent = async (message: string) => {
  const selfId = detectSelfIdentification(message);
  if (selfId) {
    return { category: selfId, source: "keyword" as const };
  }

  const aiCategory = await classifyIntentWithGemini(message);
  return {
    category: aiCategory,
    source:
      process.env.GEMINI_API_KEY || process.env.OPENAI_API_KEY
        ? ("gemini" as const)
        : ("fallback" as const),
  };
};

// ─── Conversation lookup / creation ─────────────────────────────────

const getOrCreateConversation = async (
  instagramUserId: string,
  instagramPageId: string
): Promise<IInstagramConversation> => {
  let conversation = await InstagramConversation.findOne({
    instagramUserId,
    instagramPageId,
  });

  if (!conversation) {
    log("conversation.create", { instagramUserId, instagramPageId });
    conversation = await InstagramConversation.create({
      instagramUserId,
      instagramPageId,
      currentFlow: null,
      messageStep: 0,
      tags: [],
      capturedData: {},
      messages: [
        {
          sender: "assistant",
          text: ASSISTANT_DEFAULT_MESSAGE,
          step: 0,
          delayMs: 0,
          createdAt: new Date(),
        },
      ],
      profileType: null,
      classificationSource: "fallback",
      status: "ACTIVE",
    });
  } else {
    log("conversation.loaded", {
      instagramUserId,
      instagramPageId,
      status: conversation.status,
      currentFlow: conversation.currentFlow,
      messageStep: conversation.messageStep,
      messageCount: conversation.messages.length,
    });
  }

  return conversation;
};

// ─── Core: process a single messaging event ─────────────────────────

const processMessagingEvent = async (event: MessagingEvent) => {
  const senderId = event.sender?.id;
  const recipientId = event.recipient?.id;
  const isEcho = event.message?.is_echo === true;
  const mid = event.message?.mid;

  // Handle postback (button tap) as text
  const messageText = event.message?.text ?? event.postback?.payload;

  log("event.received", {
    senderId,
    recipientId,
    hasText: Boolean(event.message?.text),
    hasPostback: Boolean(event.postback),
    isEcho,
    isRead: Boolean(event.read),
    mid,
  });

  if (event.read || isEcho || !senderId || !recipientId || !messageText) {
    log("event.skipped", {
      reason: event.read
        ? "read-receipt"
        : isEcho
          ? "echo"
          : !senderId
            ? "no-sender"
            : !recipientId
              ? "no-recipient"
              : "empty-text",
    });
    return;
  }

  if (await isDuplicateMessage(mid)) {
    log("event.skipped", { reason: "duplicate-mid", mid, senderId });
    return;
  }

  const accessToken = await metaSettingsService.getAccessToken();
  if (!accessToken) {
    logErr("event.no-access-token", new Error("Instagram access token is not configured"));
    return;
  }
  log("event.access-token-resolved", { tokenLen: accessToken.length });

  const key = `${senderId}:${recipientId}`;

  // Always append the incoming message to the per-user buffer first.
  await appendToBuffer(key, messageText, mid);
  log("buffer.appended", { senderId, recipientId, mid, textLen: messageText.length });

  // Try to become the drainer for this user.
  //
  // Two cases when acquireLock fails:
  //   - Live drainer: another Lambda is actively processing; it'll drain
  //     our message along with theirs. Safe to exit.
  //   - Crashed Lambda: the lock is orphaned. Mongo's 15s TTL will clean
  //     it up automatically. Until then, we can't proceed — but the
  //     user's *next* message will succeed and pick up everything in the
  //     buffer (buffer has its own 5-min TTL).
  //
  // We make one short retry to handle the common transient race where
  // two webhooks arrive within milliseconds. Beyond that, the TTL is the
  // recovery mechanism — anything longer would risk hitting Vercel's
  // function timeout, which would only orphan more locks.
  let acquired = await acquireLock(key);
  if (!acquired) {
    await new Promise((r) => setTimeout(r, 300));
    // If a live drainer already processed our message, the buffer will
    // be empty — nothing more to do.
    const buf = await readBuffer(key);
    if (!buf || buf.messages.length === 0) {
      log("buffer.drained-by-other", { senderId, recipientId });
      return;
    }
    acquired = await acquireLock(key);
    if (!acquired) {
      log("buffer.drainer-already-running", { senderId, recipientId });
      return;
    }
    log("buffer.lock-acquired-on-retry", { senderId, recipientId });
  }

  // Send a typing indicator immediately so the user sees the bot is
  // "thinking" during the coalesce window and AI generation.
  await sendInstagramTypingIndicator(accessToken, senderId).catch((err) => {
    logErr("typing-indicator.failed", err, { senderId });
  });

  try {
    await drainBufferLoop(senderId, recipientId, accessToken);
  } catch (error) {
    logErr("buffer.drain-failed", error, { senderId, recipientId });
  } finally {
    await releaseLock(key);
  }

  // After releasing the lock, double-check the buffer one more time.
  // It is possible a webhook appended a message in the tiny window
  // between our last buffer-empty check and lock release. If we don't
  // pick it up, that message would sit until the next webhook arrives.
  const trailing = await readBuffer(key);
  if (trailing && trailing.messages.length > 0) {
    log("buffer.trailing-message-detected", {
      senderId,
      bufferedCount: trailing.messages.length,
    });
    if (await acquireLock(key)) {
      try {
        await drainBufferLoop(senderId, recipientId, accessToken);
      } catch (error) {
        logErr("buffer.trailing-drain-failed", error, { senderId, recipientId });
      } finally {
        await releaseLock(key);
      }
    }
    // If we can't reacquire the lock, another Lambda already did — they'll handle it.
  }
};

// ─── Coalesce drain loop ─────────────────────────────────────────────
// Only one Lambda per user runs this at a time (gated by ConversationLock).
// Sleep for COALESCE_WINDOW_MS to catch any rapid follow-up messages, then
// drain whatever's in the buffer with a single combined reply. After the
// reply, if more messages arrived during AI generation, loop and handle
// the next batch the same way.
const drainBufferLoop = async (
  senderId: string,
  recipientId: string,
  accessToken: string
) => {
  const key = `${senderId}:${recipientId}`;
  const startedAt = Date.now();

  // Initial coalesce wait — gives rapid follow-ups a chance to land before
  // we read and reply. Short enough that single-message latency stays low.
  await new Promise((r) => setTimeout(r, COALESCE_WINDOW_MS));

  while (true) {
    const messages = await snapshotAndClearBuffer(key);

    if (messages.length === 0) {
      log("buffer.empty-on-drain", { senderId });
      await deleteBufferIfEmpty(key);
      return;
    }

    const combined = combineBurst(messages);

    log("buffer.draining", {
      senderId,
      bufferedCount: messages.length,
      combinedLen: combined.length,
    });

    if (!combined) {
      await deleteBufferIfEmpty(key);
      return;
    }

    try {
      await processIncomingMessage(senderId, recipientId, combined, accessToken);
    } catch (err) {
      logErr("buffer.process-failed", err, { senderId, recipientId });
    }

    // Check whether new messages arrived while we were generating the reply.
    // If so, drain them in a fresh batch — without another coalesce wait,
    // since the user already waited through our AI generation time.
    const after = await readBuffer(key);
    if (!after || after.messages.length === 0) {
      await deleteBufferIfEmpty(key);
      return;
    }
    log("buffer.new-messages-during-process", {
      senderId,
      bufferedCount: after.messages.length,
    });
    if (Date.now() - startedAt >= DRAIN_MAX_TOTAL_MS) {
      log("buffer.drain-hard-timeout", { senderId, totalElapsed: Date.now() - startedAt });
      return;
    }
  }
};

// ─── Gate checks + AI response ──────────────────────────────────────

const processIncomingMessage = async (
  senderId: string,
  pageId: string,
  messageText: string,
  accessToken: string
) => {
  log("incoming.start", { senderId, pageId, messageTextLen: messageText.length });
  const conversation = await getOrCreateConversation(senderId, pageId);

  // Process the message through the AI pipeline (same logic as chatbot).
  // Typing indicator was already sent by the drainer at burst start.
  await processAIResponse(senderId, messageText, conversation, accessToken);
  log("incoming.done", { senderId });
};

// ─── AI pipeline (mirrors chatbot conversation.sendMessage) ─────────

const processAIResponse = async (
  senderId: string,
  messageText: string,
  conversation: IInstagramConversation,
  accessToken: string
) => {
  const trimmedMessage = messageText.trim();
  if (!trimmedMessage) {
    log("ai.skip.empty", { senderId });
    return;
  }

  log("ai.start", {
    senderId,
    status: conversation.status,
    currentFlow: conversation.currentFlow,
    messageStep: conversation.messageStep,
  });

  // Record user message
  conversation.messages.push(buildUserMessage(trimmedMessage));

  if (!conversation.tags.includes("NEW")) {
    conversation.tags = addUniqueTags(conversation.tags, ["NEW"]);
  }

  const contactData = extractContactData(trimmedMessage);

  // ── Completed conversation ──
  if (conversation.status === "COMPLETED") {
    log("ai.branch.completed", {
      senderId,
      currentFlow: conversation.currentFlow,
      hasEmail: Boolean(contactData.email),
      hasPhone: Boolean(contactData.phone),
      note: "no reply will be sent — conversation already COMPLETED",
    });
    if (conversation.currentFlow && conversation.currentFlow !== "GENERAL") {
      if (contactData.email) {
        conversation.capturedData.email = contactData.email;
        conversation.tags = addUniqueTags(conversation.tags, ["EMAIL_RECEIVED"]);
      }
      if (contactData.phone) {
        conversation.capturedData.phone = contactData.phone;
        conversation.tags = addUniqueTags(conversation.tags, ["PHONE_RECEIVED"]);
      }
    }
    await conversation.save();
    return;
  }

  // ── Waiting for contact info ──
  if (
    conversation.status === "WAITING_FOR_CONTACT" &&
    conversation.currentFlow &&
    conversation.currentFlow !== "GENERAL"
  ) {
    log("ai.branch.waiting-for-contact", {
      senderId,
      currentFlow: conversation.currentFlow,
      hasEmail: Boolean(contactData.email),
      hasPhone: Boolean(contactData.phone),
      note: contactData.email || contactData.phone
        ? "contact received — will acknowledge and COMPLETE"
        : "no contact in message — saving silently, no reply",
    });
    if (contactData.email || contactData.phone) {
      if (contactData.email) {
        conversation.capturedData.email = contactData.email;
        conversation.tags = addUniqueTags(conversation.tags, ["EMAIL_RECEIVED"]);
      }
      if (contactData.phone) {
        conversation.capturedData.phone = contactData.phone;
        conversation.tags = addUniqueTags(conversation.tags, ["PHONE_RECEIVED"]);
      }

      const acknowledgement = contactData.email && contactData.phone
        ? "Got it — I've noted your email and phone number. Someone from our team will reach out to you soon."
        : contactData.email
          ? "Got it — I've noted your email. Someone from our team will reach out to you soon."
          : "Got it — I've noted your phone number. Someone from our team will reach out to you soon.";

      const assistantMsg = buildAssistantMessage(
        conversation.currentFlow,
        acknowledgement,
        conversation.messageStep + 1
      );
      conversation.messages.push(assistantMsg);
      conversation.messageStep += 1;
      conversation.status = "COMPLETED";

      await conversation.save();
      await sendInstagramMessage(accessToken, senderId, acknowledgement);
    } else {
      await conversation.save();
    }
    return;
  }

  // ── Intent classification (first message or flow upgrade) ──
  if (!conversation.currentFlow) {
    const intent = await resolveIntent(trimmedMessage);
    log("ai.intent.classified", {
      senderId,
      category: intent.category,
      source: intent.source,
    });
    conversation.currentFlow = intent.category;
    conversation.classificationSource = intent.source;
    conversation.profileType =
      intent.category === "GENERAL" ? "fan" : "professional";
    conversation.tags = addUniqueTags(conversation.tags, [
      ...mapIntentToTags(intent.category, trimmedMessage),
      "ENGAGED",
    ]);
  } else {
    const keywordMatch = detectSelfIdentification(trimmedMessage);
    if (keywordMatch && keywordMatch !== conversation.currentFlow) {
      conversation.currentFlow = keywordMatch;
      conversation.classificationSource = "keyword";
      conversation.profileType = "professional";
      conversation.messageStep = 0;
      conversation.tags = addUniqueTags(conversation.tags, [
        ...mapIntentToTags(keywordMatch, trimmedMessage),
      ]);
    } else if (
      conversation.currentFlow === "PRODUCER" &&
      conversation.messageStep === 1
    ) {
      const branch = detectProducerBranch(trimmedMessage);
      if (branch === "creative") {
        conversation.tags = addUniqueTags(conversation.tags, [
          "CREATIVE",
          "PRODUCER_CREATIVE",
        ]);
      }
      if (branch === "financing") {
        conversation.tags = addUniqueTags(conversation.tags, [
          "PRODUCER_FINANCING",
        ]);
      }
    }
  }

  // ── Capture contact data for professional flows ──
  if (conversation.currentFlow !== "GENERAL") {
    if (contactData.email) {
      conversation.capturedData.email = contactData.email;
      conversation.tags = addUniqueTags(conversation.tags, ["EMAIL_RECEIVED"]);
    }
    if (contactData.phone) {
      conversation.capturedData.phone = contactData.phone;
      conversation.tags = addUniqueTags(conversation.tags, ["PHONE_RECEIVED"]);
    }
  }

  // Safety: currentFlow must be set by now
  if (!conversation.currentFlow) {
    log("ai.skip.no-flow", { senderId, note: "currentFlow still null after classification — saving silently" });
    await conversation.save();
    return;
  }

  // ── Check reply limit ──
  const replyLimit = getReplyLimitForFlow(conversation.currentFlow);
  if (
    conversation.currentFlow !== "GENERAL" &&
    conversation.messageStep >= replyLimit
  ) {
    log("ai.reply-limit.hit", {
      senderId,
      currentFlow: conversation.currentFlow,
      messageStep: conversation.messageStep,
      replyLimit,
      note: "flipping to COMPLETED, no reply sent",
    });
    conversation.status = "COMPLETED";
    await conversation.save();
    return;
  }

  // ── Generate reply ──
  const nextStep = conversation.messageStep + 1;
  let producerBranch: "financing" | "creative" | null = null;

  if (conversation.currentFlow === "PRODUCER" && nextStep > 1) {
    producerBranch = detectProducerBranch(trimmedMessage);
    if (
      !conversation.tags.includes("PRODUCER_FINANCING") &&
      !conversation.tags.includes("PRODUCER_CREATIVE")
    ) {
      producerBranch = producerBranch ?? "financing";
    } else if (conversation.tags.includes("PRODUCER_CREATIVE")) {
      producerBranch = "creative";
    } else {
      producerBranch = "financing";
    }
  }

  let replyText: string;

  if (conversation.currentFlow === "GENERAL") {
    log("ai.reply.generating-general", { senderId, historyLen: conversation.messages.length });
    const aiResponse = await generateChatResponse(conversation.messages);
    if (aiResponse) {
      replyText = aiResponse;
      log("ai.reply.general-ok", { senderId, replyLen: replyText.length });
    } else {
      replyText =
        "Hmm, give me a sec on that one — could you rephrase or ask again?";
      logErr("ai.reply.general-fallback", new Error("All AI providers returned null"), { senderId });
    }
  } else {
    replyText = getFlowReply(
      conversation.currentFlow,
      nextStep,
      producerBranch
    );
    log("ai.reply.flow", {
      senderId,
      currentFlow: conversation.currentFlow,
      nextStep,
      producerBranch,
      replyLen: replyText.length,
    });
  }

  const assistantMessage = buildAssistantMessage(
    conversation.currentFlow,
    replyText,
    nextStep
  );

  conversation.messageStep = nextStep;
  conversation.messages.push(assistantMessage);

  if (shouldWaitForContact(conversation.currentFlow, nextStep)) {
    conversation.status = "WAITING_FOR_CONTACT";
    log("ai.status.waiting-for-contact", { senderId, currentFlow: conversation.currentFlow, nextStep });
  }

  await conversation.save();
  log("ai.saved", { senderId, status: conversation.status, messageStep: conversation.messageStep });

  // ── Send reply via Instagram Graph API (with retry) ──
  log("ai.send.start", { senderId, replyLen: replyText.length });
  const SEND_MAX_ATTEMPTS = 3;
  for (let attempt = 1; attempt <= SEND_MAX_ATTEMPTS; attempt++) {
    try {
      const res = await sendInstagramMessage(accessToken, senderId, replyText);
      log("ai.send.done", { senderId, ok: res.ok, status: res.status, attempt });
      if (res.ok) break;
      if (attempt < SEND_MAX_ATTEMPTS) {
        await new Promise((r) => setTimeout(r, 500 * attempt));
        continue;
      }
      logErr("ai.send.failed-final", new Error(`Send failed after ${SEND_MAX_ATTEMPTS} attempts`), {
        senderId,
        status: res.status,
      });
    } catch (err) {
      logErr("ai.send.threw", err, { senderId, attempt });
      if (attempt < SEND_MAX_ATTEMPTS) {
        await new Promise((r) => setTimeout(r, 500 * attempt));
        continue;
      }
    }
  }
};

// ─── Webhook handler entry points ───────────────────────────────────

/**
 * Handle the incoming webhook POST body from Instagram.
 * Called after immediately responding 200 to Meta.
 */
export const handleInstagramWebhook = async (payload: WebhookPayload) => {
  const entries = payload.entry ?? [];
  log("webhook.received", {
    object: payload.object,
    entryCount: entries.length,
    messagingCounts: entries.map((e) => e.messaging?.length ?? 0),
  });

  if (entries.length === 0) {
    log("webhook.no-entries", { rawPayload: payload });
    return;
  }

  for (const entry of entries) {
    if (Array.isArray(entry.messaging) && entry.messaging.length) {
      await Promise.all(
        entry.messaging.map((event) => processMessagingEvent(event))
      );
    } else {
      log("webhook.entry-no-messaging", {
        entryId: entry.id,
        keys: Object.keys(entry),
      });
    }
  }
  log("webhook.done");
};

/**
 * Verify the webhook subscription (GET request from Meta).
 */
export const verifyInstagramWebhook = async (query: {
  "hub.mode"?: string;
  "hub.verify_token"?: string;
  "hub.challenge"?: string;
}) => {
  const mode = query["hub.mode"];
  const token = query["hub.verify_token"];
  const challenge = query["hub.challenge"];

  const verifyToken = await metaSettingsService.getVerifyToken();

  log("webhook.verify.request", {
    mode,
    tokenProvided: Boolean(token),
    tokenConfigured: Boolean(verifyToken),
    tokenMatch: Boolean(verifyToken) && token === verifyToken,
  });

  if (mode === "subscribe" && token === verifyToken) {
    return { success: true, challenge };
  }

  return { success: false };
};
