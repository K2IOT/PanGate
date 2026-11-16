package com.pangate.chat.support;

import static org.awaitility.Awaitility.await;

import com.pangate.chat.core.ChatStores;
import com.pangate.contracts.testkit.AccountTopics;
import com.pangate.contracts.v1.AccountState;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * account spec §8: chat-service only puts users account-service published into conversations. ITs publish the
 * {@code AccountState} of their users on {@code account.users} and wait until the running chat-service read it.
 */
public final class KnownUsers {

  private KnownUsers() {}

  public static void publish(String bootstrap, String userId, AccountState.Status status, long updatedAtMs) {
    try (KafkaProducer<String, byte[]> p = new KafkaProducer<>(Map.of("bootstrap.servers", bootstrap),
        new StringSerializer(), new ByteArraySerializer())) {
      p.send(new ProducerRecord<>(AccountTopics.USERS, userId, AccountState.newBuilder().setUserId(userId)
          .setStatus(status).setUpdatedAtMs(updatedAtMs).build().toByteArray())).get();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /** Publishes each user ACTIVE and waits until chat-service knows all of them. */
  public static void add(String bootstrap, ConfigurableApplicationContext chat, String... userIds) {
    for (String u : userIds) {
      publish(bootstrap, u, AccountState.Status.STATUS_ACTIVE, System.currentTimeMillis());
    }
    ChatStores.UserStore users = chat.getBean(ChatStores.UserStore.class);
    for (String u : userIds) {
      await().atMost(Duration.ofSeconds(30)).until(() -> users.user(u).isPresent());
    }
  }
}
