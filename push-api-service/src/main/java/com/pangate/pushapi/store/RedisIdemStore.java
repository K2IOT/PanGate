package com.pangate.pushapi.store;

import com.pangate.pushapi.core.IdemStore;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import java.time.Duration;
import java.util.Optional;

/** Idempotency keys on Redis state (push-api spec §2.1). */
public final class RedisIdemStore implements IdemStore {

  static final String DELETE_IF_EQUALS =
      "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";

  private final RedisState redis;

  public RedisIdemStore(RedisState redis) {
    this.redis = redis;
  }

  @Override
  public Optional<String> get(String key) {
    return Optional.ofNullable(redis.await(redis.async().get(key), "GET idem"));
  }

  @Override
  public boolean setIfAbsent(String key, String value, Duration ttl) {
    return "OK".equals(redis.await(redis.async().set(key, value, SetArgs.Builder.nx().px(ttl.toMillis())), "SET NX"));
  }

  @Override
  public void set(String key, String value, Duration ttl) {
    redis.await(redis.async().set(key, value, SetArgs.Builder.px(ttl.toMillis())), "SET idem");
  }

  @Override
  public boolean deleteIfEquals(String key, String value) {
    Long n = redis.await(redis.async().<Long>eval(DELETE_IF_EQUALS, ScriptOutputType.INTEGER, new String[] {key}, value),
        "DEL idem");
    return n != null && n > 0;
  }
}
