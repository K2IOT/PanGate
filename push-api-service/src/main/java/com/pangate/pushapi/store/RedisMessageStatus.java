package com.pangate.pushapi.store;

import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.pushapi.core.MessageStatus;
import com.pangate.pushapi.core.PushApiKeys;
import io.lettuce.core.ScriptOutputType;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * {@code msg:{t}:<id>} hashes (push-api spec §4.2). Outcomes are written only to keys that exist, and
 * {@code delivered} wins over {@code offline} (a redelivery after a router restart never downgrades the status).
 */
public final class RedisMessageStatus implements MessageStatus {

  static final long GRACE_MS = 3_600_000L;

  static final String OFFLINE = """
      if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
      if redis.call('HGET', KEYS[1], 'state') == 'delivered' then return 0 end
      redis.call('HSET', KEYS[1], 'state', 'offline', 'offlineReason', ARGV[1], 'attempts', ARGV[2], 'offlineAt', ARGV[3])
      return 1""";

  static final String DELIVERED = """
      if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
      redis.call('HSET', KEYS[1], 'state', 'delivered', 'lastDeliveredAt', ARGV[1], 'lastStatus', ARGV[2])
      redis.call('HDEL', KEYS[1], 'offlineReason', 'attempts', 'offlineAt')
      redis.call('HINCRBY', KEYS[1], 'deliveredConns', 1)
      return 1""";

  private final RedisState redis;
  private final LongSupplier wallMs;

  public RedisMessageStatus(RedisState redis, LongSupplier wallMs) {
    this.redis = redis;
    this.wallMs = wallMs;
  }

  static String lower(Enum<?> e, String prefix) {
    String n = e.name();
    return (n.startsWith(prefix) ? n.substring(prefix.length()) : n).toLowerCase(Locale.ROOT);
  }

  static String target(PushRecord r) {
    return switch (r.getTargetCase()) {
      case PRINCIPAL_ID -> r.getPrincipalId();
      case DEVICE -> r.getDevice().getPrincipalId() + "/" + r.getDevice().getDeviceId();
      case TOPIC -> "topic:" + r.getTopic();
      default -> "";
    };
  }

  @Override
  public void accepted(PushRecord r) {
    String key = PushApiKeys.msg(r.getTenantId(), r.getMsgId());
    Map<String, String> f = new LinkedHashMap<>();
    f.put("state", "accepted");
    f.put("kind", lower(r.getKind(), "KIND_"));
    f.put("class", lower(r.getClass_(), "DELIVERY_CLASS_"));
    f.put("receipt", Boolean.toString(r.getReceipt()));
    f.put("target", target(r));
    f.put("acceptedAt", Long.toString(r.getAcceptedAtMs()));
    f.put("expiresAt", Long.toString(r.getExpiresAtMs()));
    long ttl = Math.max(1, r.getExpiresAtMs() - wallMs.getAsLong() + GRACE_MS);
    var a = redis.async();
    var set = a.hset(key, f);
    var exp = a.pexpire(key, ttl);
    redis.await(set, "HSET msg");
    redis.await(exp, "PEXPIRE msg");
  }

  @Override
  public Optional<Map<String, String>> get(String tenant, String msgId) {
    Map<String, String> m = redis.await(redis.async().hgetall(PushApiKeys.msg(tenant, msgId)), "HGETALL msg");
    return m == null || m.isEmpty() ? Optional.empty() : Optional.of(m);
  }

  @Override
  public void offline(OfflineRecord o) {
    PushRecord r = o.getRecord();
    redis.await(redis.async().<Long>eval(OFFLINE, ScriptOutputType.INTEGER,
        new String[] {PushApiKeys.msg(r.getTenantId(), r.getMsgId())}, lower(o.getReason(), "REASON_"),
        Integer.toString(o.getAttempts()), Long.toString(o.getAtMs())), "status offline");
  }

  @Override
  public void delivered(DeliveredRecord d) {
    redis.await(redis.async().<Long>eval(DELIVERED, ScriptOutputType.INTEGER,
        new String[] {PushApiKeys.msg(d.getTenantId(), d.getMsgId())}, Long.toString(d.getAtMs()),
        lower(d.getStatus(), "STATUS_")), "status delivered");
  }
}
