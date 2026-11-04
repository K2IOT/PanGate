package com.pangate.gateway.downstream;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class TopicPatternsTest {

  @Test
  void globStarMatchesAnyRunIncludingColon() {
    assertThat(TopicPatterns.allowed(List.of("t1:news:*"), "t1:news:vn:hn")).isTrue();
    assertThat(TopicPatterns.allowed(List.of("t1:*:alerts"), "t1:a:b:alerts")).isTrue();
    assertThat(TopicPatterns.allowed(List.of("t1:news"), "t1:news2")).isFalse();
    assertThat(TopicPatterns.allowed(List.of("t1:news"), "t1:news")).isTrue();
    assertThat(TopicPatterns.allowed(List.of("x", "t1:*"), "t1:")).isTrue();
    assertThat(TopicPatterns.allowed(List.of("t1:a*b*c"), "t1:aXbYc")).isTrue();
    assertThat(TopicPatterns.allowed(List.of("t1:a*b*c"), "t1:aXbY")).isFalse();
    assertThat(TopicPatterns.allowed(List.of(), "t1:x")).isFalse();
  }

  @Test
  void tenantPrefixRequired() {
    assertThat(TopicPatterns.inTenant("t1", "t1:x")).isTrue();
    assertThat(TopicPatterns.inTenant("t1", "t10:x")).isFalse();
    assertThat(TopicPatterns.inTenant("t1", "t1:")).isFalse();
    assertThat(TopicPatterns.inTenant("t1", "")).isFalse();
  }
}
