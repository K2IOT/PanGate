package com.pangate.chat.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Status;

class ChatHealthTest {

  final AtomicBoolean replayed = new AtomicBoolean(true);
  final AtomicBoolean jwks = new AtomicBoolean(true);
  final AtomicLong now = new AtomicLong(100_000);
  final ChatHealth h = new ChatHealth(replayed::get, jwks::get, now::get);

  @Test
  void readyOnlyWithRevocationsJwksAndARecentScyllaAnswer() {
    assertThat(h.health().getStatus()).as("scylla never answered").isEqualTo(Status.OUT_OF_SERVICE);
    h.scyllaAnswered();
    assertThat(h.health().getStatus()).isEqualTo(Status.UP);
    now.addAndGet(9_999);
    assertThat(h.health().getStatus()).isEqualTo(Status.UP);
    now.addAndGet(2);
    assertThat(h.health().getStatus()).as("no answer for 10 s").isEqualTo(Status.OUT_OF_SERVICE);
    h.scyllaAnswered();
    replayed.set(false);
    assertThat(h.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    replayed.set(true);
    jwks.set(false);
    assertThat(h.health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
  }
}
