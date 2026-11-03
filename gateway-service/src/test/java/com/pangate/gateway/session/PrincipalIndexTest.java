package com.pangate.gateway.session;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PrincipalIndexTest {

  private final PrincipalIndex index = new PrincipalIndex();

  @Test
  void multiDeviceAddRemove() {
    ConnRef a = new ConnRef(0, 1);
    ConnRef b = new ConnRef(3, 7);
    ConnRef c = new ConnRef(1, 9);

    index.add("t1:u1", a);
    index.add("t1:u1", b);
    index.add("t1:u1", c);
    assertThat(index.get("t1:u1")).containsExactlyInAnyOrder(a, b, c);

    index.remove("t1:u1", b);
    assertThat(index.get("t1:u1")).containsExactlyInAnyOrder(a, c);
    index.remove("t1:u1", a);
    index.remove("t1:u1", c);

    assertThat(index.get("t1:u1")).isEmpty();
    assertThat(index.principalCount()).isZero();
  }

  @Test
  void removingUnknownIsHarmless() {
    index.add("p", new ConnRef(0, 1));

    index.remove("p", new ConnRef(0, 2));
    index.remove("q", new ConnRef(0, 1));

    assertThat(index.get("p")).hasSize(1);
  }
}
