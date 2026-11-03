package com.pangate.gateway.loop;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeadlineWheelTest {

  static final class Node extends WheelNode {
    final String name;

    Node(String name) {
      this.name = name;
    }
  }

  private final DeadlineWheel wheel = new DeadlineWheel(128, 0);
  private final List<String> fired = new ArrayList<>();

  private void advance(long now) {
    wheel.advanceTo(now, n -> fired.add(((Node) n).name + "@" + now));
  }

  @Test
  void firesAtDeadlineNotBefore() {
    wheel.schedule(new Node("a"), 5);

    advance(4);
    assertThat(fired).isEmpty();
    advance(5);
    assertThat(fired).containsExactly("a@5");
    advance(200);
    assertThat(fired).containsExactly("a@5");
  }

  @Test
  void farDeadlineFiresOnlyWhenDue() {
    wheel.schedule(new Node("far"), 300);

    advance(299);
    assertThat(fired).isEmpty();
    advance(300);
    assertThat(fired).containsExactly("far@300");
  }

  @Test
  void catchesUpAfterStall() {
    wheel.schedule(new Node("x"), 7);
    wheel.schedule(new Node("y"), 3);
    List<String> order = new ArrayList<>();

    wheel.advanceTo(10, n -> order.add(((Node) n).name));

    assertThat(order).containsExactly("y", "x");
  }

  @Test
  void removeIsIdempotent() {
    Node n = new Node("r");
    wheel.schedule(n, 5);

    wheel.remove(n);
    wheel.remove(n);
    advance(10);

    assertThat(fired).isEmpty();
    assertThat(n.isScheduled()).isFalse();
    assertThat(wheel.size()).isZero();
  }

  @Test
  void callbackMayReschedule() {
    Node n = new Node("c");
    wheel.schedule(n, 5);
    List<Long> at = new ArrayList<>();

    for (long t = 1; t <= 12; t++) {
      long now = t;
      wheel.advanceTo(now, x -> {
        at.add(now);
        if (now == 5) {
          wheel.schedule(x, 10);
          assertThat(wheel.size()).isEqualTo(1);
        }
      });
    }

    assertThat(at).containsExactly(5L, 10L);
  }

  @Test
  void pastDeadlineFiresOnNextTick() {
    advance(20);
    wheel.schedule(new Node("late"), 10);

    advance(21);

    assertThat(fired).containsExactly("late@21");
  }

  @Test
  void reschedulingMovesNode() {
    Node n = new Node("m");
    wheel.schedule(n, 5);
    wheel.schedule(n, 8);

    advance(7);
    assertThat(fired).isEmpty();
    advance(8);
    assertThat(fired).containsExactly("m@8");
    assertThat(wheel.size()).isZero();
  }
}
