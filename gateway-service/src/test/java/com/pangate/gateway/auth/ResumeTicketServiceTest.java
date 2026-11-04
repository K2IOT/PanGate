package com.pangate.gateway.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.pangate.starter.secrets.FileSecretSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResumeTicketServiceTest {

  private static TicketKey key(int kid) {
    byte[] k = new byte[32];
    k[0] = (byte) kid;
    return new TicketKey(kid, k);
  }

  private static final TicketKeyRing RING = new TicketKeyRing(key(1), key(2), key(3));
  private static final ResumeTicket TICKET = new ResumeTicket("cell-1", "t1", "t1:u1", "d1",
      1_000L, 1_000L + 86_400L, ResumeTicketService.jtiHash("jti-1"), List.of("t1:news.*"));

  private final ResumeTicketService service = new ResumeTicketService(RING);

  @Test
  void issueThenVerifyRoundTrips() {
    String token = service.issue(TICKET);

    assertThat(service.verify(token, 2_000L)).isEqualTo(new TicketVerifyResult.Valid(TICKET));
  }

  @Test
  void tamperedByteIsInvalid() {
    byte[] raw = Base64.getUrlDecoder().decode(service.issue(TICKET));
    raw[10] ^= 1;

    assertThat(service.verify(Base64.getUrlEncoder().withoutPadding().encodeToString(raw), 2_000L))
        .isInstanceOf(TicketVerifyResult.Invalid.class);
  }

  @Test
  void unknownKidIsInvalid() {
    String token = new ResumeTicketService(new TicketKeyRing(key(7), key(8), key(9))).issue(TICKET);

    assertThat(service.verify(token, 2_000L)).isInstanceOf(TicketVerifyResult.Invalid.class);
  }

  @Test
  void verifiesTicketSignedWithPreviousKey() {
    String token = service.issue(TICKET);
    ResumeTicketService rotated = new ResumeTicketService(new TicketKeyRing(key(2), key(3), key(4)));

    assertThat(rotated.verify(token, 2_000L)).isEqualTo(new TicketVerifyResult.Valid(TICKET));
  }

  @Test
  void expiredTicketReportsExpired() {
    String token = service.issue(TICKET);

    assertThat(service.verify(token, TICKET.expSec())).isEqualTo(new TicketVerifyResult.Expired(TICKET));
  }

  @Test
  void rejectsMoreThanEightPatternsOrLongPattern() {
    assertThatThrownBy(() -> new ResumeTicket("c", "t", "t:u", "d", 0, 1, 0, Collections.nCopies(9, "t:x")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ResumeTicket("c", "t", "t:u", "d", 0, 1, 0, List.of("t:" + "x".repeat(63))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void tokenIsBase64UrlUnder2048() {
    String p = "t1:" + "x".repeat(61);
    ResumeTicket big = new ResumeTicket("cell-1", "t1", "t1:" + "u".repeat(100), "d".repeat(100), 1, 2, 3,
        Collections.nCopies(8, p));

    String token = service.issue(big);

    assertThat(token).matches("^[A-Za-z0-9_-]+$").hasSizeLessThanOrEqualTo(2048);
    assertThat(service.verify(token, 1)).isEqualTo(new TicketVerifyResult.Valid(big));
  }

  @Test
  void truncatedTokenIsInvalidNotException() {
    String token = service.issue(TICKET);

    assertThat(service.verify(token.substring(0, 20), 2_000L)).isInstanceOf(TicketVerifyResult.Invalid.class);
    assertThat(service.verify("!!!", 2_000L)).isInstanceOf(TicketVerifyResult.Invalid.class);
    assertThat(service.verify("", 2_000L)).isInstanceOf(TicketVerifyResult.Invalid.class);
  }

  @Test
  void keyRingLoadsFromSecretFiles(@TempDir Path d) throws Exception {
    Files.createDirectories(d.resolve("ticket-keys"));
    String k = Base64.getEncoder().encodeToString(new byte[32]);
    Files.writeString(d.resolve("ticket-keys/prev"), "1 " + k + "\n");
    Files.writeString(d.resolve("ticket-keys/current"), "2 " + k + "\n");
    Files.writeString(d.resolve("ticket-keys/next"), "3 " + k + "\n");

    TicketKeyRing ring = TicketKeyRing.load(new FileSecretSource(d));

    assertThat(ring.current().kid()).isEqualTo(2);
    assertThat(ring.byKid(3)).isPresent();
    assertThat(ring.byKid(4)).isEmpty();
  }

  @Test
  void keyRingRejectsShortKeys(@TempDir Path d) throws Exception {
    Files.createDirectories(d.resolve("ticket-keys"));
    for (String role : List.of("prev", "current", "next")) {
      Files.writeString(d.resolve("ticket-keys/" + role), "1 " + Base64.getEncoder().encodeToString(new byte[16]));
    }

    assertThatThrownBy(() -> TicketKeyRing.load(new FileSecretSource(d))).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void ticketFromPreviousCurrentVerifiesAfterRotation() {
    ResumeTicketService s = new ResumeTicketService(new TicketKeyRing(key(1), key(2), key(3)));
    String token = s.issue(TICKET);

    s.ring(new TicketKeyRing(key(2), key(3), key(4)));
    assertThat(s.verify(token, 2_000L)).isEqualTo(new TicketVerifyResult.Valid(TICKET));

    s.ring(new TicketKeyRing(key(3), key(4), key(5)));
    assertThat(s.verify(token, 2_000L)).isInstanceOf(TicketVerifyResult.Invalid.class);
  }

  @Test
  void ringsCompareByKeyContent() {
    assertThat(new TicketKeyRing(key(1), key(2), key(3))).isEqualTo(new TicketKeyRing(key(1), key(2), key(3)));
    assertThat(new TicketKeyRing(key(1), key(2), key(3))).isNotEqualTo(new TicketKeyRing(key(2), key(3), key(4)));
  }

  @Test
  void unreadableRingKeepsOld() {
    ResumeTicketService s = new ResumeTicketService(RING);
    io.micrometer.core.instrument.simple.SimpleMeterRegistry meters =
        new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
    com.pangate.starter.secrets.SecretSource broken = new com.pangate.starter.secrets.SecretSource() {
      @Override public byte[] get(String name) {
        return "not a key".getBytes();
      }

      @Override public String describe(String name) {
        return name;
      }
    };

    com.pangate.gateway.config.EdgeConfig.reloadTicketKeys(broken, s,
        new com.pangate.gateway.metrics.GatewayMetrics(meters));

    assertThat(s.ring()).isSameAs(RING);
    assertThat(meters.get("gw.secrets.reload").tag("name", "ticket-keys").tag("result", "failed").counter().count())
        .isEqualTo(1);
  }
}
