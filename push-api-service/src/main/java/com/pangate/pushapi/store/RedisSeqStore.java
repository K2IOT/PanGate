package com.pangate.pushapi.store;

import com.pangate.pushapi.core.SeqStore;

/** {@code INCR} on Redis state. */
public final class RedisSeqStore implements SeqStore {

  private final RedisState redis;

  public RedisSeqStore(RedisState redis) {
    this.redis = redis;
  }

  @Override
  public long incr(String key) {
    return redis.await(redis.async().incr(key), "INCR");
  }
}
