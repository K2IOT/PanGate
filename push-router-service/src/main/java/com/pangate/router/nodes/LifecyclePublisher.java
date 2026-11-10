package com.pangate.router.nodes;

/** Emits NODE_DOWN on cN.gw.lifecycle (push-router spec §3.4). */
@FunctionalInterface
public interface LifecyclePublisher {
  void nodeDown(String nodeId, long epoch);
}
