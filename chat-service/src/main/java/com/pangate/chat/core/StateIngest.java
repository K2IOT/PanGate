package com.pangate.chat.core;

import com.pangate.contracts.v1.ChatMark;
import com.pangate.contracts.v1.ChatTyping;
import com.pangate.contracts.v1.ReceiptCommand;
import java.util.Optional;
import java.util.function.LongSupplier;

/** Routes the state ops: marks to {@code chat.receipts}; typing and watch are wired in by later parts of CH3. */
public final class StateIngest implements StateCommands {

  private final MarkValidator marks;
  private final MarkSink sink;
  private final LongSupplier wallMs;
  private final TypingRelay typing;
  private final java.util.function.Consumer<String> onTyping;
  private final PresenceWatch watch;
  private final java.util.function.Consumer<String> onWatch;

  private final TokenBucketLimiter watchLimit;

  public StateIngest(MarkValidator marks, MarkSink sink) {
    this(marks, sink, System::currentTimeMillis, null, r -> { });
  }

  public StateIngest(MarkValidator marks, MarkSink sink, LongSupplier wallMs, TypingRelay typing,
      java.util.function.Consumer<String> onTyping) {
    this(marks, sink, wallMs, typing, onTyping, null, r -> { });
  }

  /** {@code typing} may be null (no typing relay); {@code onTyping} counts each outcome. */
  /** {@code watch} may be null; {@code onWatch} counts {@code kept} entries or {@code unavailable}. */
  public StateIngest(MarkValidator marks, MarkSink sink, LongSupplier wallMs, TypingRelay typing,
      java.util.function.Consumer<String> onTyping, PresenceWatch watch, java.util.function.Consumer<String> onWatch) {
    this(marks, sink, wallMs, typing, onTyping, watch, onWatch, new TokenBucketLimiter(1, 5, System::nanoTime));
  }

  /** Final review I4 (perf 3a): {@code watchLimit} per user before a watch reads any store ({@code rate_limited}). */
  public StateIngest(MarkValidator marks, MarkSink sink, LongSupplier wallMs, TypingRelay typing,
      java.util.function.Consumer<String> onTyping, PresenceWatch watch, java.util.function.Consumer<String> onWatch,
      TokenBucketLimiter watchLimit) {
    this.watchLimit = watchLimit;
    this.watch = watch;
    this.onWatch = onWatch;
    this.marks = marks;
    this.sink = sink;
    this.wallMs = wallMs;
    this.typing = typing;
    this.onTyping = onTyping;
  }

  @Override
  public String mark(String userId, String deviceId, ChatMark m, boolean read) {
    try {
      Optional<ReceiptCommand> c = marks.check(userId, deviceId, m, read, wallMs.getAsLong());
      if (c.isEmpty()) {
        return "rejected";
      }
      sink.publish(c.get());
      return "accepted";
    } catch (QueueUnavailableException | StoreUnavailableException e) {
      // final review I10: a mark is a hint (spec §3.3, §9); it must not hold the upstream partition (chat.send)
      marks.dropped(MarkValidator.UNAVAILABLE);
      return "rejected";
    }
  }

  /** The HTTP path (CH3 spec §3.3): "" when published, else the {@link MarkValidator} reason. */
  public String markHttp(String userId, String deviceId, ChatMark m, boolean read) {
    MarkValidator.Checked c = marks.validate(userId, deviceId, m, read, wallMs.getAsLong());
    if (c.command().isEmpty()) {
      return c.reason();
    }
    sink.publish(c.command().get());
    return "";
  }

  @Override
  public String typing(String userId, String deviceId, ChatTyping t) {
    if (typing == null) {
      return "ignored";
    }
    String r = typing.typing(userId, t.getConversationId());
    onTyping.accept(r);
    return r.equals("relayed") ? "accepted" : "rejected";
  }

  @Override
  public String watch(String userId, String deviceId, com.pangate.contracts.v1.PresenceWatch w) {
    if (watch == null) {
      return "ignored";
    }
    if (!watchLimit.tryAcquire(userId)) {
      onWatch.accept("rate_limited");
      return "rejected";
    }
    try {
      watch.watch(userId, deviceId, w);
      onWatch.accept("kept");
      return "accepted";
    } catch (StoreUnavailableException e) {
      onWatch.accept("unavailable");   // Redis down: presence is unknown until the client watches again (spec §9)
      return "rejected";
    }
  }
}
