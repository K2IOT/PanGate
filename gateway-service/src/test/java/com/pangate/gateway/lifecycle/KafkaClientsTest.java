package com.pangate.gateway.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.config.GatewayProperties;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

class KafkaClientsTest {

  private final KafkaClients clients = new KafkaClients(new GatewayProperties.Kafka("localhost:9092", false,
      List.of("cmd"), "c1", DataSize.ofMegabytes(128), DataSize.ofMegabytes(128)), "n1");

  @Test
  void idleTopicsNeverLeaveProducerMetadata() {
    // max.block.ms=0: a topic evicted after metadata.max.idle.ms (default 5 min) fails its next send at once.
    for (Map<String, Object> p : List.of(clients.producerProps("reliable", "all", true, 1L << 27),
        clients.producerProps("telemetry", "1", false, 1L << 27))) {
      assertThat(p.get(ProducerConfig.MAX_BLOCK_MS_CONFIG)).isEqualTo(0);
      assertThat(((Number) p.get(ProducerConfig.METADATA_MAX_IDLE_CONFIG)).longValue())
          .isGreaterThanOrEqualTo(Duration.ofDays(365).toMillis());
    }
  }
}
