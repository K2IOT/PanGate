package com.pangate.pushapi.store;

import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.pushapi.core.MessageStatus;
import com.pangate.pushapi.core.PushApiKeys;
import io.lettuce.core.ScriptOutputType;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * {@code msg:{t}:<id>} hashes (push-api spec §4.2). {@code accepted} is written before the record is produced, so
 * the key exists when an outcome arrives; outcomes are written only to keys that exist, and
 * {@code delivered} wins over {@code offline} (a redelivery after a router restart never downgrades the status).
 */
public final class RedisMessageStatus implements MessageStatus {

  static final long GRACE_MS = 3_600_000L;

  static final String OFFLINE = """
      if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
      if redis.call('HGET', KEYS[1], 'state') == 'delivered' then return 0 end
      redis.call('HSET', KEYS[1], 'state', 'offline', 'offlineReason', ARGV[1], 'attempts', ARGV[2], 'offlineAt', ARGV[3])
      return 1""";

  /** Written before the record is produced; never replaces an outcome already there (a retried accept). */
  static final String ACCEPTED = """
      redis.call('HSETNX', KEYS[1], 'state', 'accepted')
      redis.call('HSET', KEYS[1], 'kind', ARGV[1], 'class', ARGV[2], 'receipt', ARGV[3], 'target', ARGV[4],
        'acceptedAt', ARGV[5], 'expiresAt', ARGV[6])
      redis.call('PEXPIRE', KEYS[1], ARGV[7])
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
    long ttl = Math.max(1, r.getExpiresAtMs() - wallMs.getAsLong() + GRACE_MS);
    redis.await(redis.async().<Long>eval(ACCEPTED, ScriptOutputType.INTEGER, new String[] {key},
        lower(r.getKind(), "KIND_"), lower(r.getClass_(), "DELIVERY_CLASS_"), Boolean.toString(r.getReceipt()),
        target(r), Long.toString(r.getAcceptedAtMs()), Long.toString(r.getExpiresAtMs()), Long.toString(ttl)),
        "status accepted");
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
