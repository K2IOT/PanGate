package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.contracts.v1.ChatCommand;
import com.pangate.contracts.v1.ChatSend;
import com.pangate.contracts.v1.ConversationKind;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Orders the sends of one conversation (chat spec §4.4): dedup (Redis, then ScyllaDB) → one LWT reserving
 * {@code known+1..known+n} → write messages and dedup rows. {@code known} is kept in memory and re-read when missing or
 * after a fence. Not thread-safe per conversation: a conversation's commands come from one partition, one thread.
 */
public final class Sequencer {

  private final ChatStores.ConversationStore conversations;
  private final ChatStores.MessageStore messages;
  private final ChatStores.DedupStore durable;
  private final ChatStores.DedupStore cache;
  private final LongSupplier clockMs;
  private final Map<String, Long> known = new ConcurrentHashMap<>();
  private final Map<String, ConversationKind> kinds = new ConcurrentHashMap<>();

  public Sequencer(ChatStores.ConversationStore conversations, ChatStores.MessageStore messages,
      ChatStores.DedupStore durable, ChatStores.DedupStore cache, LongSupplier clockMs) {
    this.conversations = conversations;
    this.messages = messages;
    this.durable = durable;
    this.cache = cache;
    this.clockMs = clockMs;
  }

  /** Results in batch order; throws {@link FencedException} (nothing written) when the LWT fails. */
  public List<SequencedMessage> process(String convId, List<ChatCommand> batch) {
    StoredMessage[] found = new StoredMessage[batch.size()];
    Map<String, Integer> firstOfKey = new HashMap<>();
    int fresh = 0;
    for (int i = 0; i < batch.size(); i++) {
      ChatCommand c = batch.get(i);
      String key = c.getSenderId() + "|" + c.getSend().getClientMsgId();
      if (firstOfKey.containsKey(key)) {
        continue;   // same send twice in this batch: resolved below from the first one
      }
      firstOfKey.put(key, i);
      Optional<StoredMessage> prior = lookup(convId, c);
      if (prior.isPresent()) {
        found[i] = full(prior.get(), c);
      } else {
        fresh++;
      }
    }
    long base = 0;
    if (fresh > 0) {
      base = known.computeIfAbsent(convId, conversations::lastSeq);
      if (!conversations.reserveSeq(convId, base, fresh)) {
        known.remove(convId);
        throw new FencedException(convId);
      }
      known.put(convId, base + fresh);
    }
    long now = clockMs.getAsLong();
    List<SequencedMessage> out = new ArrayList<>(batch.size());
    StoredMessage[] produced = new StoredMessage[batch.size()];
    long next = base;
    for (int i = 0; i < batch.size(); i++) {
      ChatCommand c = batch.get(i);
      int first = firstOfKey.get(c.getSenderId() + "|" + c.getSend().getClientMsgId());
      if (first != i) {
        StoredMessage m = found[first] != null ? found[first] : produced[first];
        out.add(SequencedMessage.of(m, true, c.getReplyTo()));
        continue;
      }
      if (found[i] != null) {
        out.add(SequencedMessage.of(found[i], true, c.getReplyTo()));
        continue;
      }
      ChatSend s = c.getSend();
      StoredMessage m = new StoredMessage(convId, ++next, Ids.next(now), c.getSenderId(), s.getClientMsgId(),
          s.getText(), s.getReplyToMsgId(), List.copyOf(s.getMentionUserIdsList()), now);
      messages.write(m);
      durable.put(m);
      cache.put(m);
      produced[i] = m;
      out.add(SequencedMessage.of(m, false, c.getReplyTo()));
    }
    return out;
  }

  private Optional<StoredMessage> lookup(String convId, ChatCommand c) {
    Optional<StoredMessage> hit = cache.find(convId, c.getSenderId(), c.getSend().getClientMsgId());
    return hit.isPresent() ? hit : durable.find(convId, c.getSenderId(), c.getSend().getClientMsgId());
  }

  /** Dedup rows keep ids and seq only; the repeated command carries the same content, so the event is rebuilt. */
  private static StoredMessage full(StoredMessage d, ChatCommand c) {
    ChatSend s = c.getSend();
    return new StoredMessage(d.convId(), d.convSeq(), d.msgId(), d.senderId(), d.clientMsgId(), s.getText(),
        s.getReplyToMsgId(), List.copyOf(s.getMentionUserIdsList()), d.sentAtMs());
  }

  /** The conversation's kind for its events; kinds never change, so it is read once. */
  public ConversationKind kind(String convId) {
    ConversationKind k = kinds.get(convId);
    if (k == null) {
      k = conversations.get(convId).map(ChatStores.Conversation::kind)
          .orElse(ConversationKind.CONVERSATION_KIND_UNSPECIFIED);
      if (k != ConversationKind.CONVERSATION_KIND_UNSPECIFIED) {
        kinds.put(convId, k);
      }
    }
    return k;
  }

  /** Forgets {@code known} (partitions revoked). */
  public void forget() {
    known.clear();
  }
}
