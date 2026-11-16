package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.UserConv;
import com.pangate.contracts.v1.ConversationKind;

/**
 * account spec §8: when an account is deleted it leaves every group and channel (owner passed on first, channel
 * subscriptions revoked as in CH1); DMs stay. Idempotent: running it again changes nothing.
 */
public final class AccountRemoval {

  private final ChatStores.MemberStore members;
  private final Conversations conversations;

  public AccountRemoval(ChatStores.MemberStore members, Conversations conversations) {
    this.members = members;
    this.conversations = conversations;
  }

  public void removed(String userId) {
    for (UserConv uc : members.userConvs(userId, null, Integer.MAX_VALUE)) {
      if (uc.kind() != ConversationKind.CONVERSATION_KIND_DIRECT) {
        conversations.leaveDeletedAccount(userId, uc.convId());
      }
    }
  }
}
