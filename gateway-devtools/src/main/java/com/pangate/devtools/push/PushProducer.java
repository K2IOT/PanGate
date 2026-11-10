package com.pangate.devtools.push;

import com.google.protobuf.ByteString;
import com.pangate.contracts.testkit.PushHeaders;
import com.pangate.contracts.testkit.PushKeys;
import com.pangate.contracts.testkit.PushRecordRules;
import com.pangate.contracts.testkit.PushTopics;
import com.pangate.contracts.testkit.SeqKeys;
import com.pangate.contracts.v1.ControlOp;
import com.pangate.contracts.v1.DeliveryClass;
import com.pangate.contracts.v1.DeviceRef;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.PushItem;
import com.pangate.contracts.v1.PushRecord;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Builds PushRecords as Push API will (push contracts spec §1.1, §3.3) and produces them to {@code cN.push.requests}:
 * ULID msg_id, {@code INCR seq:{p}} for RELIABLE / ORDERED to a principal with seq_epoch 0, checked against the
 * testkit's rules, key and headers per convention. Each send waits for the broker's ack.
 */
public final class PushProducer implements AutoCloseable {

  /** {@code INCR} of a sequence key; {@code key} is the principal, or {@code topic:<topic>} for topic_seq. */
  @FunctionalInterface
  public interface SeqSource {
    long next(String key);
  }

  public static final long TTL_MS = 86_400_000L;
  private static final char[] CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ".toCharArray();

  private final Producer<String, byte[]> producer;
  private final String cell;
  private final SeqSource seq;
  private final LongSupplier wallMs;
  private final AutoCloseable onClose;

  public PushProducer(Producer<String, byte[]> producer, String cell, SeqSource seq, LongSupplier wallMs) {
    this(producer, cell, seq, wallMs, () -> { });
  }

  private PushProducer(Producer<String, byte[]> producer, String cell, SeqSource seq, LongSupplier wallMs,
      AutoCloseable onClose) {
    this.producer = producer;
    this.cell = cell;
    this.seq = seq;
    this.wallMs = wallMs;
    this.onClose = onClose;
  }

  /** A producer on {@code bootstrap}; seq from Redis {@code redisUri}, or none (EPHEMERAL / CONTROL only) if null. */
  public static PushProducer connect(String bootstrap, String redisUri, String cell) {
    KafkaProducer<String, byte[]> kafka = new KafkaProducer<>(java.util.Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap, ProducerConfig.ACKS_CONFIG, "all",
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true), new StringSerializer(), new ByteArraySerializer());
    if (redisUri == null || redisUri.isBlank()) {
      return new PushProducer(kafka, cell, k -> {
        throw new IllegalStateException("RELIABLE / ORDERED needs --redis for seq:{p}");
      }, System::currentTimeMillis, () -> kafka.close(Duration.ofSeconds(5)));
    }
    RedisClient redis = RedisClient.create(redisUri);
    StatefulRedisConnection<String, String> c = redis.connect();
    return new PushProducer(kafka, cell, k -> c.sync().incr(SeqKeys.seq(k)), System::currentTimeMillis, () -> {
      kafka.close(Duration.ofSeconds(5));
      c.close();
      redis.shutdown();
    });
  }

  static String tenantOf(String id) {
    int i = id.indexOf(':');
    if (i <= 0) {
      throw new IllegalArgumentException("expected <tenant>:<id>, got " + id);
    }
    return id.substring(0, i);
  }

  static boolean sequenced(DeliveryClass c) {
    return c == DeliveryClass.DELIVERY_CLASS_RELIABLE || c == DeliveryClass.DELIVERY_CLASS_ORDERED;
  }

  private PushRecord.Builder base(PushRecord.Kind kind, String tenant, DeliveryClass c, String msgId) {
    long now = wallMs.getAsLong();
    return PushRecord.newBuilder().setMsgId(msgId).setKind(kind).setTenantId(tenant).setCellId(cell).setClass_(c)
        .setPriority(PushItem.Priority.PRIORITY_NORMAL).setAcceptedAtMs(now).setExpiresAtMs(now + TTL_MS);
  }

  private static Frame.Builder frame(String msgId, DeliveryClass c, byte[] payload) {
    return Frame.newBuilder().setType(FrameType.FRAME_TYPE_PUSH).setMsgId(msgId).setClass_(c)
        .setPayload(ByteString.copyFrom(payload)).putHeaders("type", "devtools");
  }

  /** MESSAGE to every session of {@code principal}, or to one device when {@code device} is set. */
  public PushRecord message(String principal, String device, DeliveryClass c, byte[] payload, boolean receipt) {
    String id = ulid();
    PushRecord.Builder b = base(PushRecord.Kind.KIND_MESSAGE, tenantOf(principal), c, id).setReceipt(receipt);
    Frame.Builder f = frame(id, c, payload);
    if (device != null && !device.isEmpty()) {
      b.setDevice(DeviceRef.newBuilder().setPrincipalId(principal).setDeviceId(device));
    } else {
      b.setPrincipalId(principal);
      if (sequenced(c)) {
        f.setSeq(seq.next(principal)).setSeqEpoch(0);
      }
    }
    return send(b.setFrame(f).build());
  }

  public PushRecord broadcast(String topic, DeliveryClass c, byte[] payload) {
    String id = ulid();
    Frame.Builder f = frame(id, c, payload).setTopic(topic);
    if (sequenced(c)) {
      f.setTopicSeq(seq.next("topic:" + topic));
    }
    return send(base(PushRecord.Kind.KIND_BROADCAST, tenantOf(topic), c, id).setTopic(topic).setFrame(f).build());
  }

  public PushRecord control(String principal, ControlOp op) {
    PushRecord r = base(PushRecord.Kind.KIND_CONTROL, tenantOf(principal), DeliveryClass.DELIVERY_CLASS_UNSPECIFIED,
        ulid()).setPriority(PushItem.Priority.PRIORITY_CRITICAL).setPrincipalId(principal).setControl(op).build();
    return send(r);
  }

  /** Validates, produces and waits for the ack. */
  public PushRecord send(PushRecord r) {
    PushRecordRules.assertValid(r);
    ProducerRecord<String, byte[]> pr =
        new ProducerRecord<>(PushTopics.requests(cell).name(), PushKeys.of(r), r.toByteArray());
    PushHeaders.write(pr.headers(), r.getTenantId(), cell, null, "PushRecord");
    try {
      producer.send(pr).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted producing " + r.getMsgId(), e);
    } catch (ExecutionException e) {
      throw new IllegalStateException("produce of " + r.getMsgId() + " failed: " + e.getCause(), e.getCause());
    }
    return r;
  }

  private String ulid() {
    char[] out = new char[26];
    long t = wallMs.getAsLong();
    for (int i = 9; i >= 0; i--) {
      out[i] = CROCKFORD[(int) (t & 31)];
      t >>>= 5;
    }
    ThreadLocalRandom r = ThreadLocalRandom.current();
    for (int i = 10; i < 26; i++) {
      out[i] = CROCKFORD[r.nextInt(32)];
    }
    return new String(out);
  }

  @Override
  public void close() {
    try {
      onClose.close();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
