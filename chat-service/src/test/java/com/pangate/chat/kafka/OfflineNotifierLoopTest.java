package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.core.Badge;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.NotifySink;
import com.pangate.chat.core.OfflineNotifications;
import com.pangate.chat.core.PrivacyCache;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.NotifyRequest;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Perf 3c (spec §4.5): offline records run one lane per recipient; a failed send is retried in place. */
class OfflineNotifierLoopTest {

  static final String T = "c1.push.offline";
  final TopicPartition tp = new TopicPartition(T, 0);
  final FakeStores stores = new FakeStores();
  final String g = Ids.next();
  final List<NotifyRequest> sent = Collections.synchronizedList(new ArrayList<>());
  final AtomicInteger seeks = new AtomicInteger();
  final MockConsumer<String, byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
    @Override
    public synchronized void seek(TopicPartition p, long offset) {
      seeks.incrementAndGet();
      super.seek(p, offset);
    }
  };
  OfflineNotifierLoop loop;

  OfflineNotifierLoopTest() {
    stores.create(new Conversation(g, ConversationKind.CONVERSATION_KIND_GROUP, "Nhóm", "u1", 0, false, 3),
        List.of(new Member("u1", Role.OWNER), new Member("u2", Role.MEMBER), new Member("u3", Role.MEMBER)));
    stores.put("u1", "active", "Lan", 1);
  }

  void start(NotifySink sink) {
    loop = new OfflineNotifierLoop(consumer, T, new OfflineNotifications(stores, stores,
        new PrivacyCache(stores, () -> 0L), stores, new Badge(stores), Duration.ofHours(1)), sink,
        new SimpleMeterRegistry());
    consumer.schedulePollTask(() -> {
      consumer.rebalance(List.of(tp));
      consumer.updateBeginningOffsets(new HashMap<>(Map.of(tp, 0L)));
    });
    loop.start();
    await().until(() -> !consumer.assignment().isEmpty());
  }

  void offline(long offset, String recipient) {
    ChatMessage m = ChatMessage.newBuilder().setConversationId(g).setMsgId(Ids.next()).setConvSeq(1)
        .setSenderId("u1").setText("hi").setSentAtMs(System.currentTimeMillis()).build();
    OfflineRecord r = OfflineRecord.newBuilder().setRecord(PushRecord.newBuilder().setTenantId("chat")
        .setPrincipalId("chat:" + recipient).setFrame(Frame.newBuilder().setPayload(m.toByteString())
            .putHeaders("type", "chat.message"))).setReason(OfflineRecord.Reason.REASON_NO_SESSION).build();
    consumer.addRecord(new ConsumerRecord<>(T, 0, offset, "chat:" + recipient, r.toByteArray()));
  }

  @AfterEach
  void stop() {
    if (loop != null) {
      loop.close();
    }
  }

  @Test
  void recipientsAreNotifiedInParallel() {
    start(q -> {
      sent.add(q);
      return q.getPrincipalId().equals("chat:u2") ? new CompletableFuture<>()   // never acked
          : CompletableFuture.completedFuture(null);
    });
    offline(0, "u2");
    await().until(() -> sent.size() == 1);
    offline(1, "u3");
    await().until(() -> sent.stream().anyMatch(q -> q.getPrincipalId().equals("chat:u3")));
  }

  @Test
  void failedSendIsRetriedInPlace() {
    AtomicInteger failures = new AtomicInteger(2);
    start(q -> {
      sent.add(q);
      return failures.getAndDecrement() > 0
          ? CompletableFuture.failedFuture(new org.apache.kafka.common.errors.TimeoutException("broker away"))
          : CompletableFuture.completedFuture(null);
    });
    offline(0, "u2");
    await().pollInterval(Duration.ofMillis(5)).until(() -> sent.size() >= 1);
    seeks.set(0);   // MockConsumer seeks once itself, to set the first position
    await().until(() -> sent.size() == 3);
    assertThat(sent.stream().map(NotifyRequest::getRequestId).distinct()).hasSize(1);
    assertThat(seeks.get()).isZero();
  }
}
