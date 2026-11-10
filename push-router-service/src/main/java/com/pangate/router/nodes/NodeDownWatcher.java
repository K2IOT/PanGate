package com.pangate.router.nodes;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * NODE_DOWN once per {@code node_id:epoch} when a node this router has seen loses its lease or changes epoch
 * (push-router spec §3.4). Nodes never seen alive by this router produce nothing.
 */
public final class NodeDownWatcher implements BiConsumer<Map<String, Node>, Map<String, Node>> {

  private final LifecyclePublisher publisher;
  private final Set<String> emitted = ConcurrentHashMap.newKeySet();

  public NodeDownWatcher(LifecyclePublisher publisher) {
    this.publisher = publisher;
  }

  @Override
  public void accept(Map<String, Node> old, Map<String, Node> now) {
    for (Node o : old.values()) {
      Node n = now.get(o.id());
      if ((n == null || n.epoch() != o.epoch()) && emitted.add(o.id() + ":" + o.epoch())) {
        publisher.nodeDown(o.id(), o.epoch());
      }
    }
  }
}
