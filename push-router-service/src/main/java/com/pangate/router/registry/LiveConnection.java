package com.pangate.router.registry;

import java.util.Optional;

/** One live registry entry: the connection, its node and epoch, its device (empty for v1 entries). */
public record LiveConnection(String connId, String nodeId, long epoch, Optional<String> device) {}
