package com.pangate.router.nodes;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class NodeDirectoryTest {

  final AtomicLong mono = new AtomicLong(1_000);
  final AtomicReference<Object> next = new AtomicReference<>(Map.of());
  @SuppressWarnings("unchecked")
  final NodeSource source = now -> {
    Object o = next.get();
    if (o instanceof Exception e) {
      throw e;
    }
    return (Map<String, Node>) o;
  };
  final NodeDirectory dir = new NodeDirectory(source, () -> 0L, mono::get, Duration.ofSeconds(10));

  @Test
  void refreshKeepsTheLastSnapshotOnFailure() {
    next.set(Map.of("a", new Node("a", 1, "h:1", false)));
    dir.refresh();
    next.set(new IllegalStateException("redis down"));
    dir.refresh();
    assertThat(dir.live()).containsOnlyKeys("a");
    assertThat(dir.node("a")).hasValue(new Node("a", 1, "h:1", false));
  }

  @Test
  void freshTurnsFalseAfter10sWithoutSuccess() {
    assertThat(dir.fresh()).isFalse();
    dir.refresh();
    assertThat(dir.fresh()).isTrue();
    next.set(new IllegalStateException("down"));
    mono.addAndGet(10_001);
    dir.refresh();
    assertThat(dir.fresh()).isFalse();
  }

  @Test
  void drainingNodesStayLiveAndListenersSeeOldAndNew() {
    List<String> seen = new ArrayList<>();
    dir.onChange((o, n) -> seen.add(o.keySet() + "->" + n.keySet()));
    next.set(Map.of("a", new Node("a", 1, "h:1", true)));
    dir.refresh();
    assertThat(dir.live()).containsKey("a");
    assertThat(dir.node("a").orElseThrow().draining()).isTrue();
    assertThat(seen).containsExactly("[]->[a]");
  }

  /** Final review #2: BROADCAST must not "deliver" to no nodes while the directory is blind. */
  @Test
  void requireFreshThrowsWhenStale() {
    org.assertj.core.api.Assertions.assertThatThrownBy(dir::requireFresh)
        .isInstanceOf(com.pangate.router.registry.RegistryUnavailableException.class);
    dir.refresh();
    assertThat(dir.requireFresh()).isEqualTo(dir.live());
  }
}
