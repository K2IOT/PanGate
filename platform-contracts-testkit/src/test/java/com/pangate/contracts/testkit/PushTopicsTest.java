package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class PushTopicsTest {

  @Test
  void topicSpecsMatchSpecSection2() {
    assertThat(PushTopics.requests("c1")).isEqualTo(new PushTopics.Spec("c1.push.requests", 64, Duration.ofHours(24)));
    assertThat(PushTopics.offline("c1")).isEqualTo(new PushTopics.Spec("c1.push.offline", 16, Duration.ofDays(7)));
    assertThat(PushTopics.delivered("c1")).isEqualTo(new PushTopics.Spec("c1.push.delivered", 16, Duration.ofHours(24)));
    assertThat(PushTopics.lifecycle("c1")).isEqualTo(new PushTopics.Spec("c1.gw.lifecycle", 32, Duration.ofHours(24)));
    assertThat(PushTopics.all("c2")).extracting(PushTopics.Spec::name)
        .containsExactly("c2.push.requests", "c2.push.offline", "c2.push.delivered", "c2.gw.lifecycle");
  }
}
