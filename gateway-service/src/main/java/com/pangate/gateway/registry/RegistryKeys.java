package com.pangate.gateway.registry;

/** Redis keys (spec §6.1–§6.7). Braces are hash tags: each script touches exactly one key, cluster-safe. */
public final class RegistryKeys {

  public static final String NODES_LIVE = "nodes:live";
  public static final String SEQ_EPOCHS = "seqepochs";

  private RegistryKeys() {}

  public static String epoch(String nodeId) {
    return "node:epoch:{" + nodeId + "}";
  }

  public static String node(String nodeId) {
    return "node:{" + nodeId + "}";
  }

  public static String reg(String principalId) {
    return "reg:{" + principalId + "}";
  }

  public static String seq(String principalId) {
    return "seq:{" + principalId + "}";
  }
}
