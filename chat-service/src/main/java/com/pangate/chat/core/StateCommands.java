package com.pangate.chat.core;

import com.pangate.contracts.v1.ChatMark;
import com.pangate.contracts.v1.ChatTyping;
import com.pangate.contracts.v1.PresenceWatch;

/**
 * The conversation-state ops of the upstream (CH3 spec §2): {@code chat.delivered}, {@code chat.read},
 * {@code chat.typing}, {@code chat.presence.watch}. Each returns the listener's outcome ({@code accepted},
 * {@code rejected} or {@code ignored}); none answers the client.
 */
public interface StateCommands {

  String mark(String userId, String deviceId, ChatMark m, boolean read);

  String typing(String userId, String deviceId, ChatTyping t);

  String watch(String userId, String deviceId, PresenceWatch w);

  StateCommands NONE = new StateCommands() {
    @Override
    public String mark(String userId, String deviceId, ChatMark m, boolean read) {
      return "ignored";
    }

    @Override
    public String typing(String userId, String deviceId, ChatTyping t) {
      return "ignored";
    }

    @Override
    public String watch(String userId, String deviceId, PresenceWatch w) {
      return "ignored";
    }
  };
}
