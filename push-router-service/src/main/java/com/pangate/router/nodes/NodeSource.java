package com.pangate.router.nodes;

import java.util.Map;

/** Reads the live nodes of the cell (push-router spec §3.2). */
@FunctionalInterface
public interface NodeSource {
  Map<String, Node> fetch(long nowMs) throws Exception;
}
