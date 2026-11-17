package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Marks;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.ReceiptCommand;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * CH3 spec §3.4 step 1–2: the marks of a conversation, merged per user (max of each column, read raising
 * delivered), until its window ends: DM 500 ms, group 2 s, channel at once. Not thread-safe: one per worker loop.
 */
public final class ReceiptAggregator {

  public record Flush(String convId, ConversationKind kind, Map<String, Marks> byUser) {}

  private static final class Pending {
    final ConversationKind kind;
    final long dueAt;
    final Map<String, Marks> byUser = new LinkedHashMap<>();

    Pending(ConversationKind kind, long dueAt) {
      this.kind = kind;
      this.dueAt = dueAt;
    }
  }

  private final LongSupplier monoMs;
  private final long dmMs;
  private final long groupMs;
  private final Map<String, Pending> byConv = new HashMap<>();

  public ReceiptAggregator(LongSupplier monoMs, Duration dmWindow, Duration groupWindow) {
    this.monoMs = monoMs;
    this.dmMs = dmWindow.toMillis();
    this.groupMs = groupWindow.toMillis();
  }

  public void add(ReceiptCommand c, ConversationKind kind) {
    long window = switch (kind) {
      case CONVERSATION_KIND_DIRECT -> dmMs;
      case CONVERSATION_KIND_CHANNEL -> 0;
      default -> groupMs;
    };
    Pending p = byConv.computeIfAbsent(c.getConversationId(), k -> new Pending(kind, monoMs.getAsLong() + window));
    long read = c.getReadSeq();
    long delivered = Math.max(c.getDeliveredSeq(), read);
    p.byUser.merge(c.getUserId(), new Marks(delivered, read),
        (o, n) -> new Marks(Math.max(o.deliveredSeq(), n.deliveredSeq()), Math.max(o.readSeq(), n.readSeq())));
  }

  /** The conversations whose window ended, removed from the aggregator. */
  public List<Flush> due() {
    long now = monoMs.getAsLong();
    List<Flush> out = new ArrayList<>();
    for (Iterator<Map.Entry<String, Pending>> it = byConv.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<String, Pending> e = it.next();
      if (now >= e.getValue().dueAt) {
        out.add(new Flush(e.getKey(), e.getValue().kind, Map.copyOf(e.getValue().byUser)));
        it.remove();
      }
    }
    return out;
  }

  public boolean pending(String convId) {
    return byConv.containsKey(convId);
  }

  public void clear() {
    byConv.clear();
  }
}
