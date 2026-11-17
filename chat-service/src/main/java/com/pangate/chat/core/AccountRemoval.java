package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.UserConv;
import com.pangate.contracts.v1.ConversationKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * account spec §8: when an account is deleted it leaves every group and channel (owner passed on first, channel
 * subscriptions revoked as in CH1); DMs stay. Idempotent: running it again changes nothing. A conversation that
 * refuses ({@link ChatRefusal}, deterministic) is logged and skipped so the others are still left; a store failure
 * propagates so the whole record is retried.
 */
public final class AccountRemoval {

  private static final Logger log = LoggerFactory.getLogger(AccountRemoval.class);

  private final ChatStores.MemberStore members;
  private final Conversations conversations;

  public AccountRemoval(ChatStores.MemberStore members, Conversations conversations) {
    this.members = members;
    this.conversations = conversations;
  }

  public void removed(String userId) {
    for (UserConv uc : members.userConvs(userId, null, Integer.MAX_VALUE)) {
      if (uc.kind() != ConversationKind.CONVERSATION_KIND_DIRECT) {
        try {
          conversations.leaveDeletedAccount(userId, uc.convId());
        } catch (ChatRefusal e) {
          log.warn("deleted account could not leave conversation {}: {}", uc.convId(), e.getMessage());
        }
      }
    }
  }
}
