package com.pangate.chat.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.core.ChatStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.support.ChatStack;
import com.pangate.chat.support.KnownUsers;
import com.pangate.contracts.v1.AccountState;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** account spec §8: chat-service knows users from the compacted account.users. */
class AccountsListenerIT {

  static ChatStack stack;

  @BeforeAll
  static void start() throws Exception {
    stack = new ChatStack().start();
  }

  @AfterAll
  static void stop() {
    if (stack != null) {
      stack.close();
    }
  }

  int dm(String a, String b) {
    return stack.call("POST", "/v1/dm/" + b, a, null).statusCode();
  }

  /** Review Focus 5. */
  @Test
  void newUserIsKnownWithinTwoSeconds() {
    String a = Ids.next();
    String b = Ids.next();
    KnownUsers.add(stack.kafka.getBootstrapServers(), stack.chat, a);
    assertThat(dm(a, b)).isEqualTo(404);
    assertThat(stack.json(stack.call("POST", "/v1/dm/" + b, a, null)).get("code").asText())
        .isEqualTo("user_not_found");
    KnownUsers.publish(stack.kafka.getBootstrapServers(), b, AccountState.Status.STATUS_ACTIVE,
        System.currentTimeMillis());
    long t0 = System.nanoTime();
    await().atMost(Duration.ofSeconds(2)).pollInterval(Duration.ofMillis(50)).until(() -> dm(a, b) == 200);
    assertThat((System.nanoTime() - t0) / 1_000_000).isLessThan(2_000);
  }

  @Test
  void olderStateDoesNotOverwrite() {
    String a = Ids.next();
    String b = Ids.next();
    KnownUsers.add(stack.kafka.getBootstrapServers(), stack.chat, a);
    KnownUsers.publish(stack.kafka.getBootstrapServers(), b, AccountState.Status.STATUS_DELETED, 2_000);
    KnownUsers.publish(stack.kafka.getBootstrapServers(), b, AccountState.Status.STATUS_ACTIVE, 1_000);
    ChatStores.UserStore users = stack.chat.getBean(ChatStores.UserStore.class);
    await().atMost(Duration.ofSeconds(10)).until(() -> users.user(b).isPresent());
    await().during(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(3))
        .until(() -> users.user(b).get().status().equals("deleted"));
    assertThat(stack.json(stack.call("POST", "/v1/dm/" + b, a, null)).get("code").asText())
        .isEqualTo("user_deleted");
  }

  @Test
  void deletedUserLeavesGroupsAndItsDmRefusesSends() {
    String owner = Ids.next();
    String m1 = Ids.next();
    String friend = Ids.next();
    KnownUsers.add(stack.kafka.getBootstrapServers(), stack.chat, owner, m1, friend);
    String g = stack.json(stack.call("POST", "/v1/conversations", owner, Map.of("kind", "group", "title", "g",
        "memberIds", List.of(m1)))).get("id").asText();
    String dm = stack.json(stack.call("POST", "/v1/dm/" + owner, friend, null)).get("id").asText();
    KnownUsers.publish(stack.kafka.getBootstrapServers(), owner, AccountState.Status.STATUS_DELETED,
        System.currentTimeMillis());
    ChatStores.MemberStore members = stack.chat.getBean(ChatStores.MemberStore.class);
    await().atMost(Duration.ofSeconds(10)).until(() -> members.member(g, owner).isEmpty());
    assertThat(members.member(g, m1)).get().extracting(ChatStores.Member::role).isEqualTo(ChatStores.Role.OWNER);
    var r = stack.call("POST", "/v1/conversations/" + dm + "/messages", stack.token(friend, "d1"),
        Map.of("text", "still there?"), Map.of("Idempotency-Key", Ids.next()));
    assertThat(r.statusCode()).isEqualTo(404);
    assertThat(stack.json(r).get("code").asText()).isEqualTo("user_deleted");
  }

  @Test
  void replayFromTheStartRebuildsUsers() throws Exception {
    String a = Ids.next();
    KnownUsers.add(stack.kafka.getBootstrapServers(), stack.chat, a);
    var cql = stack.chat.getBean(com.datastax.oss.driver.api.core.CqlSession.class);
    cql.execute("TRUNCATE chat.users");
    ChatStores.UserStore users = stack.chat.getBean(ChatStores.UserStore.class);
    assertThat(users.user(a)).isEmpty();
    try (AccountsListener again = new AccountsListener(AccountsListener.consumer(stack.kafka.getBootstrapServers(),
        "chat-accounts-replay-" + Ids.next()), com.pangate.contracts.testkit.AccountTopics.USERS, users,
        u -> { })) {
      again.start();
      await().atMost(Duration.ofSeconds(20)).until(() -> users.user(a).isPresent());
    }
  }
}
