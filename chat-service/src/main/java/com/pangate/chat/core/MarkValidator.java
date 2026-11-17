package com.pangate.chat.core;

import com.pangate.contracts.v1.ChatMark;
import com.pangate.contracts.v1.ReceiptCommand;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * CH3 spec §3.3: a delivered / read mark from a member, clamped to the conversation's {@code last_seq}, at most
 * 50/s (burst 100) per user per pod. A mark that fails is dropped and counted; it is a hint the client sends again.
 * Read implies delivered (spec §3.1).
 */
public final class MarkValidator {

  public static final String INVALID = "invalid";
  public static final String NOT_MEMBER = "not_member";
  public static final String RATE_LIMITED = "rate_limited";

  private final MemberLookup members;
  private final LastSeqCache lastSeq;
  private final TokenBucketLimiter perUser;
  private final Consumer<String> onReject;

  public MarkValidator(MemberLookup members, LastSeqCache lastSeq, TokenBucketLimiter perUser,
      Consumer<String> onReject) {
    this.members = members;
    this.lastSeq = lastSeq;
    this.perUser = perUser;
    this.onReject = onReject;
  }

  /** The command, or why not (one of {@link #INVALID}, {@link #NOT_MEMBER}, {@link #RATE_LIMITED}). */
  public record Checked(Optional<ReceiptCommand> command, String reason) {}

  public Optional<ReceiptCommand> check(String userId, String deviceId, ChatMark m, boolean read, long nowMs) {
    Checked c = validate(userId, deviceId, m, read, nowMs);
    if (c.command().isEmpty()) {
      onReject.accept(c.reason());
    }
    return c.command();
  }

  public Checked validate(String userId, String deviceId, ChatMark m, boolean read, long nowMs) {
    if (!Ids.isUlid(m.getConversationId()) || m.getUpToSeq() < 1 || userId == null || userId.isEmpty()) {
      return refused(INVALID);
    }
    if (members.of(m.getConversationId()).stream().noneMatch(x -> x.userId().equals(userId))) {
      return refused(NOT_MEMBER);
    }
    if (!perUser.tryAcquire(userId)) {
      return refused(RATE_LIMITED);
    }
    long seq = Math.min(m.getUpToSeq(), lastSeq.lastSeq(m.getConversationId()));
    if (seq < 1) {
      return refused(INVALID);
    }
    ReceiptCommand.Builder c = ReceiptCommand.newBuilder().setUserId(userId).setDeviceId(deviceId == null ? "" : deviceId)
        .setConversationId(m.getConversationId()).setDeliveredSeq(seq).setReceivedAtMs(nowMs);
    if (read) {
      c.setReadSeq(seq);
    }
    return new Checked(Optional.of(c.build()), "");
  }

  private static Checked refused(String reason) {
    return new Checked(Optional.empty(), reason);
  }
}
