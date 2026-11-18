package com.pangate.chat.core;

import com.pangate.contracts.v1.ChatMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** CH4 spec §4.2: of a page of followers, those who get the post: not the sender, not online, not muted. */
public final class ChannelNotifyPlan {

  private ChannelNotifyPlan() {}

  public static List<String> recipients(List<ChatStores.MemberMute> page, Set<String> online, ChatMessage m,
      long nowMs) {
    List<String> out = new ArrayList<>();
    for (ChatStores.MemberMute f : page) {
      if (f.userId().equals(m.getSenderId()) || online.contains(f.userId())) {
        continue;
      }
      if (Mutes.shouldNotify(f.mutedUntil(), m.getMentionUserIdsList(), f.userId(), nowMs)) {
        out.add(f.userId());
      }
    }
    return out;
  }
}
