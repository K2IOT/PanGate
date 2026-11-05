package com.pangate.gateway.registry;

import com.pangate.gateway.downstream.ControlPushHandler;
import com.pangate.gateway.loop.LoopContext;
import com.pangate.gateway.loop.LoopRegistry;
import com.pangate.gateway.server.NodeState;
import com.pangate.gateway.session.ConnRef;
import com.pangate.gateway.session.PrincipalIndex;
import com.pangate.gateway.session.Session;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Single-session mode (spec §6.6): the registry entries a new connection replaced are kicked. An entry of this very
 * incarnation (same nodeId and epoch) is kicked locally on the loops holding the principal; any other entry goes once
 * per principal to the Push API, which routes KICK to the node that has it.
 */
public final class SingleSessionKicker implements BiConsumer<Session, Map<String, String>> {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SingleSessionKicker.class);

  private final PrincipalIndex principals;
  private final LoopRegistry loops;
  private final NodeState node;
  private final ControlPushHandler control;
  private final KickClient client;

  public SingleSessionKicker(PrincipalIndex principals, LoopRegistry loops, NodeState node,
      ControlPushHandler control, KickClient client) {
    this.principals = principals;
    this.loops = loops;
    this.node = node;
    this.control = control;
    this.client = client;
  }

  @Override
  public void accept(Session s, Map<String, String> previous) {
    boolean local = false;
    boolean remote = false;
    for (Map.Entry<String, String> e : previous.entrySet()) {
      String[] parts = e.getValue().split("\\|", -1);
      long epoch;
      try {
        epoch = parts.length >= 2 ? Long.parseLong(parts[1]) : -1;
      } catch (NumberFormatException ex) {
        epoch = -1;
      }
      if (epoch < 0) {
        log.warn("ignoring malformed registry entry for connection {}", e.getKey());
      } else if (parts[0].equals(node.nodeId()) && epoch == node.epoch()) {
        local = true;
      } else {
        remote = true;
      }
    }
    String principal = s.identity().principalId();
    String connId = s.connId();
    long connectedAtMs = s.connectedAtMs();
    if (local) {
      boolean[] seen = new boolean[loops.size()];
      for (ConnRef ref : principals.get(principal)) {
        if (ref.loopIdx() < seen.length && !seen[ref.loopIdx()]) {
          seen[ref.loopIdx()] = true;
          LoopContext loop = loops.get(ref.loopIdx());
          loop.eventLoop().execute(() -> control.kick(loop, principal, connId, connectedAtMs));
        }
      }
    }
    if (remote) {
      client.kick(s.identity().tenantId(), principal, connId, connectedAtMs);
    }
  }
}
