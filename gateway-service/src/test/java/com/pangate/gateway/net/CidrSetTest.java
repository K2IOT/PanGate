package com.pangate.gateway.net;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.util.List;
import org.junit.jupiter.api.Test;

class CidrSetTest {

  @Test
  void matchesV4AndV6Prefixes() throws Exception {
    CidrSet s = CidrSet.parse(List.of("10.0.0.0/8", "2001:db8::/32", "192.0.2.7"));
    assertThat(s.contains(InetAddress.getByName("10.1.2.3"))).isTrue();
    assertThat(s.contains(InetAddress.getByName("11.0.0.1"))).isFalse();
    assertThat(s.contains(InetAddress.getByName("2001:db8::1"))).isTrue();
    assertThat(s.contains(InetAddress.getByName("2001:db9::1"))).isFalse();
    assertThat(s.contains(InetAddress.getByName("192.0.2.7"))).isTrue();
    assertThat(s.contains(InetAddress.getByName("192.0.2.8"))).isFalse();
  }

  @Test
  void nonByteAlignedPrefix() throws Exception {
    CidrSet s = CidrSet.parse(List.of("172.16.0.0/12"));
    assertThat(s.contains(InetAddress.getByName("172.31.255.255"))).isTrue();
    assertThat(s.contains(InetAddress.getByName("172.32.0.0"))).isFalse();
  }

  @Test
  void rejectsMalformedEntry() {
    assertThatThrownBy(() -> CidrSet.parse(List.of("10.0.0.0/33")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("10.0.0.0/33");
    assertThatThrownBy(() -> CidrSet.parse(List.of("not-an-ip/8")))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not-an-ip/8");
  }
}
