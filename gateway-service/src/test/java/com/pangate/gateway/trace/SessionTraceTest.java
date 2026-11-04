package com.pangate.gateway.trace;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.pangate.contracts.v1.FrameType;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.LoopFixture;
import java.net.InetAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class SessionTraceTest {

  private final AtomicLong nanos = new AtomicLong();

  @AfterEach
  void reset() {
    SessionTrace.clock(System::nanoTime);
  }

  @Test
  void linesAreCappedAtOneThousandPerSecond() {
    LoopFixture f = new LoopFixture();
    Session s = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    s.activate(new Session.Identity("t1", "t1:u1", "d1", "ios", "1.0", Set.of()), "CONN", 0L, f.wallNow(),
        f.wallNow() + 3600, 1, List.of());
    SessionTrace.clock(nanos::get);
    Logger log = (Logger) LoggerFactory.getLogger("pangate.trace");
    ListAppender<ILoggingEvent> lines = new ListAppender<>();
    lines.start();
    log.addAppender(lines);
    try {
      for (int i = 0; i < 1500; i++) {
        SessionTrace.frame(s, "in", FrameType.FRAME_TYPE_PING, "", 2);
      }
      assertThat(lines.list).hasSize(SessionTrace.MAX_LINES_PER_SEC);
      nanos.addAndGet(1_000_000_000L);
      SessionTrace.frame(s, "in", FrameType.FRAME_TYPE_PING, "", 2);
      assertThat(lines.list).hasSize(SessionTrace.MAX_LINES_PER_SEC + 1);
    } finally {
      log.detachAppender(lines);
    }
  }
}
