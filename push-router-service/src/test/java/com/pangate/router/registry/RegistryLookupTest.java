package com.pangate.router.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.contracts.testkit.RegistryValues;
import com.pangate.router.nodes.Node;
import com.pangate.router.nodes.NodeDirectory;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class RegistryLookupTest {

  final AtomicLong mono = new AtomicLong();
  final Map<String, Node> nodes = new HashMap<>();
  final NodeDirectory dir = new NodeDirectory(now -> nodes, () -> 0L, mono::get, Duration.ofSeconds(10));
  final Map<String, Map<String, String>> reg = new HashMap<>();
  final AtomicInteger fetches = new AtomicInteger();

  RegistryLookup lookup() {
    return new RegistryLookup(p -> {
      fetches.incrementAndGet();
      return CompletableFuture.completedFuture(reg.getOrDefault(p, Map.of()));
    }, dir, Duration.ofSeconds(2), Duration.ofMillis(200), mono::get);
  }

  @Test
  void parsesEveryReferenceCase() {
    var lookup = lookup();
    for (var c : RegistryValues.cases()) {
      reg.clear();
      nodes.clear();
      c.expected().ifPresent(p -> nodes.put(p.nodeId(), new Node(p.nodeId(), p.epoch(), "h:1", false)));
      dir.refresh();
      reg.put("t1:u1", Map.of("C1", c.value()));
      lookup.invalidate("t1:u1");
      var live = lookup.live("t1:u1");
      if (c.expected().isPresent()) {
        assertThat(live).as(c.name()).singleElement().satisfies(l -> {
          assertThat(l.nodeId()).isEqualTo(c.expected().get().nodeId());
          assertThat(l.device()).isEqualTo(c.expected().get().device());
        });
      } else {
        assertThat(live).as(c.name()).isEmpty();
      }
    }
  }

  @Test
  void dropsStaleEpochsDrainingNodesAndUnknownNodes() {
    nodes.put("a", new Node("a", 3, "h:1", false));
    nodes.put("d", new Node("d", 1, "h:2", true));
    dir.refresh();
    reg.put("t1:u1", Map.of("C1", "a|3|1|ios|d1", "C2", "a|2|1|ios|d2", "C3", "d|1|1|ios|d3", "C4", "z|1|1|ios|d4"));
    assertThat(lookup().live("t1:u1")).extracting(LiveConnection::connId).containsExactly("C1");
  }

  @Test
  void cachesForTwoSecondsAndInvalidateForcesAFetch() {
    nodes.put("a", new Node("a", 3, "h:1", false));
    dir.refresh();
    reg.put("t1:u1", Map.of("C1", "a|3|1|ios|d1"));
    var lookup = lookup();
    lookup.live("t1:u1");
    lookup.live("t1:u1");
    assertThat(fetches).hasValue(1);
    mono.addAndGet(2_001);
    lookup.live("t1:u1");
    assertThat(fetches).hasValue(2);
    lookup.invalidate("t1:u1");
    lookup.live("t1:u1");
    assertThat(fetches).hasValue(3);
  }

  @Test
  void timeoutRaisesRegistryUnavailable() {
    var lookup = new RegistryLookup(p -> new CompletableFuture<>(), dir, Duration.ofSeconds(2),
        Duration.ofMillis(200), mono::get);
    assertThatThrownBy(() -> lookup.live("t1:u1")).isInstanceOf(RegistryUnavailableException.class);
  }
}
