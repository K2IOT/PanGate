package com.pangate.gateway.downstream;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.LoopFixture;
import java.net.InetAddress;
import java.util.List;
import org.junit.jupiter.api.Test;

class TopicIndexTest {

  private final LoopFixture f = new LoopFixture();
  private final TopicIndex index = f.loop.topics();

  private Session session() {
    return f.tracker.admit(new io.netty.channel.embedded.EmbeddedChannel(), f.loop, InetAddress.getLoopbackAddress());
  }

  @Test
  void addRemoveAndSnapshotIsACopy() {
    Session a = session();
    Session b = session();
    assertThat(index.add(a, "t1:x")).isTrue();
    assertThat(index.add(a, "t1:x")).isFalse();
    index.add(b, "t1:x");
    List<Session> snap = index.snapshot("t1:x");
    assertThat(snap).containsExactly(a, b);
    assertThat(a.topics()).containsExactly("t1:x");
    assertThat(index.remove(a, "t1:x")).isTrue();
    assertThat(index.remove(a, "t1:x")).isFalse();
    assertThat(snap).containsExactly(a, b);
    assertThat(index.snapshot("t1:x")).containsExactly(b);
    assertThat(a.topics()).isNull();
    assertThat(index.snapshot("none")).isEmpty();
  }

  @Test
  void removeAllClearsEveryTopicOfSession() {
    Session a = session();
    index.add(a, "t1:x");
    index.add(a, "t1:y");
    index.removeAll(a);
    assertThat(index.topicCount()).isZero();
    assertThat(a.topics()).isNull();
  }
}
