package com.pangate.gateway.session;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.test.LoopFixture;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SessionTest {

  private final LoopFixture f = new LoopFixture();

  private Session admit() {
    return f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
  }

  private static Session.Identity id() {
    return new Session.Identity("t1", "t1:u1", "d1", "ios", "1.0", Set.of());
  }

  @Test
  void handshakeTimesOutAfter10s() {
    Session s = admit();

    f.advanceTo(f.now() + 9);
    assertThat(f.channel.isOpen()).isTrue();
    f.advanceTo(f.now() + 1);

    assertThat(f.channel.isOpen()).isFalse();
    assertThat(s.closeCode()).isEqualTo(CloseCode.HELLO_TIMEOUT);
  }

  @Test
  void helloDeadlineIs5sAfterUpgrade() {
    Session s = admit();
    f.channel.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.WS);
    f.channel.attr(ChannelAttrs.PROTOCOL_READY).set(true);
    f.advanceTo(f.now() + 2);
    s.markUpgraded(f.now());

    f.advanceTo(f.now() + 4);
    assertThat(f.channel.isOpen()).isTrue();
    f.advanceTo(f.now() + 1);

    assertThat(s.closeCode()).isEqualTo(CloseCode.HELLO_TIMEOUT);
    CloseWebSocketFrame close = f.channel.readOutbound();
    assertThat(close.statusCode()).isEqualTo(4004);
    close.release();
  }

  @Test
  void idleActiveSessionClosesAfter90s() {
    Session s = admit();
    s.activate(id(), "CONN", f.now(), f.now() + 86_400, 0, List.of());

    f.advanceTo(f.now() + 89);
    assertThat(f.channel.isOpen()).isTrue();
    f.advanceTo(f.now() + 1);

    assertThat(f.channel.isOpen()).isFalse();
    assertThat(s.closeCode()).isEqualTo(CloseCode.GOING_AWAY);
    assertThat(s.closeReason()).isEqualTo("HEARTBEAT_TIMEOUT");
  }

  @Test
  void touchPostponesIdleTimeout() {
    Session s = admit();
    s.activate(id(), "CONN", f.now(), f.now() + 86_400, 0, List.of());
    long start = f.now();

    f.advanceTo(start + 60);
    s.touch(f.now());
    f.advanceTo(start + 140);
    assertThat(f.channel.isOpen()).isTrue();
    f.advanceTo(start + 150);

    assertThat(f.channel.isOpen()).isFalse();
  }

  @Test
  void expiryClosesWith4002() {
    Session s = admit();
    s.activate(id(), "CONN", f.now(), f.now() + 50, 0, List.of());
    long start = f.now();

    for (long t = start + 10; t <= start + 50; t += 10) {
      f.advanceTo(t);
      s.touch(t);
    }

    assertThat(s.closeCode()).isEqualTo(CloseCode.SESSION_EXPIRED);
  }

  @Test
  void closeIsIdempotent() {
    Session s = admit();

    s.close(CloseCode.BAD_FRAME, "a");
    s.close(CloseCode.INTERNAL, "b");

    assertThat(s.closeCode()).isEqualTo(CloseCode.BAD_FRAME);
    assertThat(s.state()).isEqualTo(SessionState.CLOSED);
  }

  @Test
  void activationRegistersPrincipalAndCloseCleansUp() {
    Session s = admit();
    s.activate(id(), "CONN", f.now(), f.now() + 86_400, 0, List.of());
    f.tracker.onActivated(s);
    assertThat(f.principals.get("t1:u1")).containsExactly(new ConnRef(0, s.sessionKey()));

    f.channel.close();
    f.channel.runPendingTasks();

    assertThat(f.principals.principalCount()).isZero();
    assertThat(f.loop.activeCount()).isZero();
    assertThat(f.loop.sessionCount()).isZero();
    assertThat(f.ips.trackedKeys()).isZero();
    assertThat(s.closeCode()).isEqualTo(CloseCode.ABNORMAL);
  }
}
