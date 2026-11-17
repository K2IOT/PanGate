package com.pangate.contracts.testkit;

import com.pangate.contracts.v1.ChatMark;
import com.pangate.contracts.v1.PresenceWatch;
import java.util.ArrayList;
import java.util.List;

/**
 * Validation cases for {@code ChatMark} and {@code PresenceWatch} (CH3 spec §3.3, §6.3), the rules that need no
 * stored state. {@code reason} is what chat-service counts a dropped command as; empty means valid. Data only.
 */
public final class ChatStateCases {

  public record MarkCase(String name, ChatMark mark, String reason) {}

  public record WatchCase(String name, PresenceWatch watch, String reason) {}

  public static final String CONV = ChatSendCases.CONV;
  public static final String USER = "01JD0U5ER00000000000000000";
  public static final int MAX_WATCH = 200;

  private ChatStateCases() {}

  private static ChatMark.Builder mark() {
    return ChatMark.newBuilder().setConversationId(CONV).setUpToSeq(7);
  }

  public static List<MarkCase> validMarks() {
    return List.of(new MarkCase("valid", mark().build(), ""),
        new MarkCase("up_to_seq 1", mark().setUpToSeq(1).build(), ""));
  }

  public static List<MarkCase> invalidMarks() {
    return List.of(new MarkCase("empty conversation_id", mark().setConversationId("").build(), "invalid"),
        new MarkCase("conversation_id not ULID", mark().setConversationId("conv-1").build(), "invalid"),
        new MarkCase("up_to_seq 0", mark().setUpToSeq(0).build(), "invalid"));
  }

  private static PresenceWatch.Entry entry(int i) {
    return PresenceWatch.Entry.newBuilder().setUserId(USER.substring(0, 23) + String.format("%03d", i))
        .setConversationId(CONV).build();
  }

  private static PresenceWatch entries(int n) {
    PresenceWatch.Builder b = PresenceWatch.newBuilder();
    for (int i = 0; i < n; i++) {
      b.addEntries(entry(i));
    }
    return b.build();
  }

  public static List<WatchCase> validWatches() {
    return List.of(new WatchCase("one entry", entries(1), ""),
        new WatchCase("200 entries", entries(MAX_WATCH), ""),
        new WatchCase("empty = stop watching", PresenceWatch.getDefaultInstance(), ""));
  }

  public static List<WatchCase> invalidWatches() {
    List<WatchCase> c = new ArrayList<>();
    c.add(new WatchCase("201 entries", entries(MAX_WATCH + 1), "invalid"));
    c.add(new WatchCase("empty user_id", PresenceWatch.newBuilder()
        .addEntries(PresenceWatch.Entry.newBuilder().setConversationId(CONV)).build(), "invalid"));
    c.add(new WatchCase("conversation_id not ULID", PresenceWatch.newBuilder()
        .addEntries(PresenceWatch.Entry.newBuilder().setUserId(USER).setConversationId("conv-1")).build(),
        "invalid"));
    return c;
  }
}
