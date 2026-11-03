package com.pangate.gateway.session;

/**
 * Notified by {@link SessionTracker} on the session's owning event loop: after a session became ACTIVE (indexed),
 * and after an activated session's channel closed (all bookkeeping done). Implementations must not block.
 */
public interface SessionListener {

  default void activated(Session s) {}

  default void closed(Session s) {}
}
