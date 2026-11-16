package com.pangate.account;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jwt.SignedJWT;
import com.pangate.account.application.port.AuthApi;
import com.pangate.account.application.port.Outbox;
import com.pangate.account.application.port.Transactions;
import com.pangate.account.domain.Ids;
import com.pangate.account.support.AccountIT;
import com.pangate.account.support.ControlTopic;
import com.pangate.contracts.v1.AccountState;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@AccountIT
class RelaysIT {

  @Autowired Outbox outbox;
  @Autowired Transactions tx;
  @Autowired AuthApi auth;

  static AccountState state(String u, AccountState.Status s, long at) {
    return AccountState.newBuilder().setUserId(u).setStatus(s).setUpdatedAtMs(at).build();
  }

  @Test
  void eventsReachTheCompactedTopicInOrder() throws Exception {
    String u = Ids.next();
    tx.inTx(() -> {
      outbox.event(u, state(u, AccountState.Status.STATUS_ACTIVE, 1));
      outbox.event(u, state(u, AccountState.Status.STATUS_DELETED, 2));
    });
    List<ConsumerRecord<String, byte[]>> recs = ControlTopic.await("account.users",
        rs -> rs.stream().filter(r -> u.equals(r.key())).count() >= 2, Duration.ofSeconds(20)).stream()
        .filter(r -> u.equals(r.key())).toList();
    assertThat(recs).hasSize(2);
    assertThat(AccountState.parseFrom(recs.get(0).value()).getStatus()).isEqualTo(AccountState.Status.STATUS_ACTIVE);
    assertThat(AccountState.parseFrom(recs.get(1).value()).getStatus()).isEqualTo(AccountState.Status.STATUS_DELETED);
  }

  @Test
  void revocationsReachGwControlThroughAuth() {
    String u = Ids.next();
    tx.inTx(() -> outbox.revokeDevice(u, "D9"));
    assertThat(ControlTopic.hasKey("gw.control", "revoke:d:chat:" + u + "/D9", Duration.ofSeconds(20))).isTrue();
  }

  @Test
  void mintedTokenHasTheSessionDevice() throws Exception {
    String u = Ids.next();
    AuthApi.ConnectToken t = auth.mint(u, "DEV1");
    var claims = SignedJWT.parse(t.token()).getJWTClaimsSet();
    assertThat(claims.getSubject()).isEqualTo("chat:" + u);
    assertThat(claims.getStringClaim("device")).isEqualTo("DEV1");
    assertThat(claims.getAudience()).containsExactly("pangate-gw");
    assertThat(claims.getExpirationTime().getTime() - claims.getIssueTime().getTime()).isEqualTo(600_000);
    assertThat(t.expiresAt()).isAfter(java.time.Instant.now());
  }
}
