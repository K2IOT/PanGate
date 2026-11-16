package com.pangate.account;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.account.application.port.Outbox;
import com.pangate.account.application.port.Transactions;
import com.pangate.account.domain.Ids;
import com.pangate.account.support.AccountIT;
import com.pangate.account.support.ControlTopic;
import com.pangate.contracts.v1.AccountState;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

/**
 * account spec §6: the two relays are independent; Kafka down for account-service does not hold revocations. Own
 * database: relays of the other cached IT contexts (live Kafka) would otherwise send this test's rows.
 */
@AccountIT
@TestPropertySource(properties = {"account.kafka.bootstrap=127.0.0.1:1", "it.database=kafkadown"})
class RelaysKafkaDownIT {

  @Autowired Outbox outbox;
  @Autowired Transactions tx;

  @Test
  void kafkaDownDoesNotDelayRevocations() {
    String u = Ids.next();
    tx.inTx(() -> {
      outbox.event(u, AccountState.newBuilder().setUserId(u).setStatus(AccountState.Status.STATUS_ACTIVE).build());
      outbox.revokeDevice(u, "D1");
    });
    long t0 = System.nanoTime();
    var seen = ControlTopic.await("gw.control", rs -> rs.stream().anyMatch(r -> ("revoke:d:chat:" + u + "/D1")
        .equals(r.key())), Duration.ofSeconds(15));
    assertThat(seen.stream().anyMatch(r -> ("revoke:d:chat:" + u + "/D1").equals(r.key())))
        .as("after %d ms, %d records, outbox revocations pending %d", (System.nanoTime() - t0) / 1_000_000,
            seen.size(), outbox.pendingCount("revocation")).isTrue();
    assertThat(outbox.pending("event", 10_000).stream().anyMatch(r -> r.key().equals(u))).isTrue();
  }
}
