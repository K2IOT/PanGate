package com.pangate.router.links;

/** The node is not (or no longer) in the live directory. */
public final class NodeUnavailableException extends RuntimeException {
  public NodeUnavailableException(String nodeId) {
    super("gateway node " + nodeId + " is not live", null, false, false);
  }
}
