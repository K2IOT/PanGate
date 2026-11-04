package com.pangate.gateway.registry;

/** Sends KICK for a single-session principal whose previous connection lives on another node (spec §6.6). */
@FunctionalInterface
public interface KickClient {

  /** Fire-and-forget; never called on an event loop's critical path and never blocks. */
  void kick(String tenant, String principal, String newConnId, long connectedAtMs);

  /** {@code gateway.push-api.enabled=false} (local): entries on other nodes are only logged. */
  static KickClient disabled() {
    org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(KickClient.class);
    return (t, p, c, at) -> log.warn("single-session entry on another node not kicked: push-api disabled");
  }
}
