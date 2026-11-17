package com.pangate.chat.core;

import com.pangate.contracts.v1.ChatMark;
import com.pangate.contracts.v1.ChatTyping;
import com.pangate.contracts.v1.PresenceWatch;
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

  public StateIngest(MarkValidator marks, MarkSink sink) {
    this(marks, sink, System::currentTimeMillis, null, r -> { });
  }

  /** {@code typing} may be null (no typing relay); {@code onTyping} counts each outcome. */
  public StateIngest(MarkValidator marks, MarkSink sink, LongSupplier wallMs, TypingRelay typing,
      java.util.function.Consumer<String> onTyping) {
    this.marks = marks;
    this.sink = sink;
    this.wallMs = wallMs;
    this.typing = typing;
    this.onTyping = onTyping;
  }

  @Override
  public String mark(String userId, String deviceId, ChatMark m, boolean read) {
    Optional<ReceiptCommand> c = marks.check(userId, deviceId, m, read, wallMs.getAsLong());
    if (c.isEmpty()) {
      return "rejected";
    }
    sink.publish(c.get());
    return "accepted";
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
  public String watch(String userId, String deviceId, PresenceWatch w) {
    return "ignored";
  }
}
