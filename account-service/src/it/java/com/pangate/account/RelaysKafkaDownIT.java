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

/** account spec §6: the two relays are independent; Kafka down for account-service does not hold revocations. */
@AccountIT
@TestPropertySource(properties = "account.kafka.bootstrap=127.0.0.1:1")
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
    assertThat(ControlTopic.hasKey("gw.control", "revoke:d:chat:" + u + "/D1", Duration.ofSeconds(15))).isTrue();
    assertThat(outbox.pending("event", 10_000).stream().anyMatch(r -> r.key().equals(u))).isTrue();
  }
}
