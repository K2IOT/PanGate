package com.pangate.gateway.config;

/** {@code standalone} in local and dev, {@code cluster} in staging and prod (spec §1, §9.1). */
public enum RedisTopology {
  STANDALONE,
  CLUSTER
}
