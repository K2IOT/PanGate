package com.pangate.gateway.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class PendingAckWindowTest {

  private static PendingEntry e(String id, long at, int bytes) {
    return new PendingEntry(id, id.hashCode(), null, at, bytes);
  }

  @Test
  void fitsByCountAndBytes() {
    PendingAckWindow w = new PendingAckWindow(2, 100);
    w.add(e("a", 1, 60));
    assertThat(w.fits(40)).isTrue();
    assertThat(w.fits(41)).isFalse();
    w.add(e("b", 1, 40));
    assertThat(w.fits(1)).isFalse();
    assertThat(w.size()).isEqualTo(2);
  }

  @Test
  void removeKeepsRingOrder() {
    PendingAckWindow w = new PendingAckWindow(32, 1000);
    w.add(e("a", 10, 1));
    w.add(e("b", 11, 1));
    w.add(e("c", 12, 1));
    assertThat(w.remove("b").msgId()).isEqualTo("b");
    assertThat(w.remove("b")).isNull();
    assertThat(w.oldestSentAtSec()).isEqualTo(10);
    List<String> drained = new ArrayList<>();
    w.drain(x -> drained.add(x.msgId()));
    assertThat(drained).containsExactly("a", "c");
    assertThat(w.size()).isZero();
    assertThat(w.oldestSentAtSec()).isEqualTo(Long.MAX_VALUE);
    assertThat(w.fits(1000)).isTrue();
  }

  @Test
  void expireTakesOnlyOverdue() {
    PendingAckWindow w = new PendingAckWindow(32, 1000);
    w.add(e("a", 10, 1));
    w.add(e("b", 13, 1));
    List<String> out = new ArrayList<>();
    w.expire(15, 5, x -> out.add(x.msgId()));
    assertThat(out).containsExactly("a");
    w.expire(18, 5, x -> out.add(x.msgId()));
    assertThat(out).containsExactly("a", "b");
  }

  @Test
  void replaceRetargetsDeliveryKeepingSendTime() {
    PendingAckWindow w = new PendingAckWindow(32, 1000);
    w.add(e("a", 10, 1));
    w.replace("a", 99, null);
    assertThat(w.find("a").deliveryId()).isEqualTo(99);
    assertThat(w.find("a").sentAtSec()).isEqualTo(10);
  }
}
