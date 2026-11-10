package com.pangate.contracts.testkit;

import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Test-only stand-in for push-router: every MESSAGE goes offline (NO_SESSION); receipt=true also gets delivered. */
final class FakeRouter implements AutoCloseable {

  private final Thread thread;
  private volatile boolean running = true;

  FakeRouter(String bootstrap, String cell) {
    thread = Thread.ofPlatform().daemon().name("fake-router").start(() -> run(bootstrap, cell));
  }

  private void run(String bootstrap, String cell) {
    try (var consumer = new KafkaConsumer<String, byte[]>(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
        ConsumerConfig.GROUP_ID_CONFIG, "fake-router", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
        new StringDeserializer(), new ByteArrayDeserializer());
        var producer = new KafkaProducer<String, byte[]>(Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap),
            new StringSerializer(), new ByteArraySerializer())) {
      consumer.subscribe(List.of(PushTopics.requests(cell).name()));
      while (running) {
        for (var rec : consumer.poll(Duration.ofMillis(200))) {
          PushRecord r;
          try {
            r = PushRecord.parseFrom(rec.value());
          } catch (Exception e) {
            continue;
          }
          if (r.getKind() != PushRecord.Kind.KIND_MESSAGE) {
            continue;
          }
          OfflineRecord o = Fixtures.offline(r, OfflineRecord.Reason.REASON_NO_SESSION);
          var out = new ProducerRecord<>(PushTopics.offline(cell).name(), PushKeys.of(o), o.toByteArray());
          PushHeaders.write(out.headers(), r.getTenantId(), cell, null, "OfflineRecord");
          producer.send(out);
          if (r.getReceipt()) {
            var d = Fixtures.delivered(r, "conn-1");
            var dr = new ProducerRecord<>(PushTopics.delivered(cell).name(), PushKeys.of(d), d.toByteArray());
            PushHeaders.write(dr.headers(), r.getTenantId(), cell, null, "DeliveredRecord");
            producer.send(dr);
          }
        }
      }
    }
  }

  @Override
  public void close() throws InterruptedException {
    running = false;
    thread.join(5_000);
  }
}
