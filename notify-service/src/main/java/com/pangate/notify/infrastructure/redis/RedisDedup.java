package com.pangate.notify.infrastructure.redis;

import com.pangate.notify.application.port.Dedup;
import io.lettuce.core.SetArgs;

/** {@code SET notifydedup:<id> 1 NX EX 3600} (offline notifications spec §5.4). Throws when Redis is down. */
public final class RedisDedup implements Dedup {

  private final LazyRedis redis;

  public RedisDedup(LazyRedis redis) {
    this.redis = redis;
  }

  @Override
  public boolean firstTime(String requestId) {
    return "OK".equals(redis.sync().set("notifydedup:" + requestId, "1", SetArgs.Builder.nx().ex(3600)));
  }
}
