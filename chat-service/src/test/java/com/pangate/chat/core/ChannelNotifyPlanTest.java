package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.MemberMute;
import com.pangate.contracts.v1.ChatMessage;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** CH4 spec §4.2: who of a page of followers gets the post. */
class ChannelNotifyPlanTest {

  static final long NOW = 1_800_000_000_000L;

  static ChatMessage post(String... mentions) {
    return ChatMessage.newBuilder().setConversationId("C").setMsgId("M").setSenderId("own").setText("t")
        .setSentAtMs(NOW).addAllMentionUserIds(List.of(mentions)).build();
  }

  @Test
  void skipsSenderOnlineAndMutedKeepsOrder() {
    List<MemberMute> page = List.of(new MemberMute("a", 0), new MemberMute("b", Long.MAX_VALUE),
        new MemberMute("c", 0), new MemberMute("own", 0), new MemberMute("d", NOW - 1));
    assertThat(ChannelNotifyPlan.recipients(page, Set.of("c"), post(), NOW)).containsExactly("a", "d");
  }

  @Test
  void aMutedFollowerWhoIsMentionedStillGetsIt() {
    List<MemberMute> page = List.of(new MemberMute("b", Long.MAX_VALUE));
    assertThat(ChannelNotifyPlan.recipients(page, Set.of(), post("b"), NOW)).containsExactly("b");
  }
}
