import admin from "./admin";
import business from "./business";
import conversation from "./conversation";
import conversationLock from "./conversationLock";
import instagramConversation from "./instagramConversation";
import messageBuffer from "./messageBuffer";
import metaSettings from "./metaSettings";
import processedMessage from "./processedMessage";
import user from "./user";

const db = {
  admin,
  business,
  conversation,
  conversationLock,
  instagramConversation,
  messageBuffer,
  metaSettings,
  processedMessage,
  user,
};

export default db;
