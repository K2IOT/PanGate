package com.pangate.gateway.shed;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import org.junit.jupiter.api.Test;

class IpConnectionCounterTest {

  private final IpConnectionCounter counter = new IpConnectionCounter();

  @Test
  void enforcesLimitPerIpv4Address() throws Exception {
    InetAddress a = InetAddress.getByName("10.0.0.1");

    assertThat(counter.tryIncrement(a, 2)).isTrue();
    assertThat(counter.tryIncrement(a, 2)).isTrue();
    assertThat(counter.tryIncrement(a, 2)).isFalse();
    assertThat(counter.tryIncrement(InetAddress.getByName("10.0.0.2"), 2)).isTrue();
    assertThat(counter.count(a)).isEqualTo(2);
  }

  @Test
  void ipv6SameSlash64SharesCounter() throws Exception {
    InetAddress a = InetAddress.getByName("2001:db8::1");
    InetAddress b = InetAddress.getByName("2001:db8::ffff");
    InetAddress other = InetAddress.getByName("2001:db8:0:1::1");

    assertThat(counter.tryIncrement(a, 1)).isTrue();
    assertThat(counter.tryIncrement(b, 1)).isFalse();
    assertThat(counter.tryIncrement(other, 1)).isTrue();
    assertThat(IpConnectionCounter.key(a)).isEqualTo(IpConnectionCounter.key(b));
  }

  @Test
  void ipv4MappedIpv6CountsAsIpv4() throws Exception {
    byte[] mapped = new byte[16];
    mapped[10] = (byte) 0xff;
    mapped[11] = (byte) 0xff;
    mapped[12] = 10;
    mapped[15] = 1;
    InetAddress v6 = java.net.Inet6Address.getByAddress(null, mapped, -1);

    assertThat(IpConnectionCounter.key(v6)).isEqualTo(IpConnectionCounter.key(InetAddress.getByName("10.0.0.1")));
  }

  @Test
  void decrementToZeroRemovesKey() throws Exception {
    InetAddress a = InetAddress.getByName("10.0.0.1");
    counter.tryIncrement(a, 5);
    counter.tryIncrement(a, 5);

    counter.decrement(a);
    counter.decrement(a);

    assertThat(counter.trackedKeys()).isZero();
    assertThat(counter.count(a)).isZero();
  }

  @Test
  void decrementUnknownIsHarmless() throws Exception {
    counter.decrement(InetAddress.getByName("10.9.9.9"));

    assertThat(counter.trackedKeys()).isZero();
  }
}
