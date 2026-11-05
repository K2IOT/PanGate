package com.pangate.gateway.session;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.test.LoopFixture;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

class SessionTrackerListenersTest {

  private final LoopFixture f = new LoopFixture();
  private final List<String> events = new CopyOnWriteArrayList<>();

  private Session activeSession() {
    Session s = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    s.activate(new Session.Identity("t1", "t1:u1", "d1", "ios", "1.0", Set.of()), "CONN", 42L, f.wallNow(),
        f.wallNow() + 3_600, 0, List.of());
    f.tracker.onActivated(s);
    return s;
  }

  @Test
  void listenersSeeActivationAndCloseOnce() {
    f.listeners.add(new SessionListener() {
      @Override
      public void activated(Session s) {
        events.add("activated " + s.connId() + " " + s.connectedAtMs());
      }

      @Override
      public void closed(Session s) {
        events.add("closed " + s.connId());
      }
    });
    Session s = activeSession();

    s.close(com.pangate.gateway.protocol.CloseCode.NORMAL, "bye");
    f.channel.runPendingTasks();
    f.channel.close();
    f.channel.runPendingTasks();

    assertThat(events).containsExactly("activated CONN 42", "closed CONN");
  }

  @Test
  void handshakingSessionCloseIsNotReported() {
    f.listeners.add(new SessionListener() {
      @Override
      public void closed(Session s) {
        events.add("closed");
      }
    });
    f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());

    f.channel.close();
    f.channel.runPendingTasks();

    assertThat(events).isEmpty();
  }

  @Test
  void throwingListenerDoesNotBreakCleanup() {
    f.listeners.add(new SessionListener() {
      @Override
      public void activated(Session s) {
        throw new IllegalStateException("boom");
      }

      @Override
      public void closed(Session s) {
        throw new IllegalStateException("boom");
      }
    });
    activeSession();

    f.channel.close();
    f.channel.runPendingTasks();

    assertThat(f.loop.activeCount()).isZero();
    assertThat(f.principals.principalCount()).isZero();
  }
}
