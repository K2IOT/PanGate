package com.pangate.gateway.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class KernelStatsReaderTest {

  private static final Path DIR = Path.of("src/test/resources/proc");

  @Test
  void parsesTcpExtCountersByName() {
    KernelStatsReader r = new KernelStatsReader(DIR.resolve("netstat"), DIR.resolve("sockstat"));
    r.refresh();
    assertThat(r.tcpExt("ListenOverflows")).isEqualTo(17);
    assertThat(r.tcpExt("ListenDrops")).isEqualTo(23);
    assertThat(r.tcpExt("TCPBacklogDrop")).isEqualTo(5);
  }

  @Test
  void parsesSockstatTcpMem() {
    KernelStatsReader r = new KernelStatsReader(DIR.resolve("netstat"), DIR.resolve("sockstat"));
    r.refresh();
    assertThat(r.tcpMemPages()).isEqualTo(412);
  }

  @Test
  void missingFieldOrFileReadsZeroWithoutThrowing() {
    KernelStatsReader r = new KernelStatsReader(DIR.resolve("nope"), DIR.resolve("nope2"));
    r.refresh();
    assertThat(r.tcpExt("ListenOverflows")).isZero();
    assertThat(r.tcpMemPages()).isZero();
    KernelStatsReader ok = new KernelStatsReader(DIR.resolve("netstat"), DIR.resolve("sockstat"));
    ok.refresh();
    assertThat(ok.tcpExt("NoSuchField")).isZero();
  }

  @Test
  void countersAndGaugeAreBound() {
    KernelStatsReader r = new KernelStatsReader(DIR.resolve("netstat"), DIR.resolve("sockstat"));
    r.refresh();
    SimpleMeterRegistry meters = new SimpleMeterRegistry();
    r.bindTo(meters);
    assertThat(meters.get("gw.kernel.listen.overflows").functionCounter().count()).isEqualTo(17);
    assertThat(meters.get("gw.kernel.listen.drops").functionCounter().count()).isEqualTo(23);
    assertThat(meters.get("gw.kernel.tcp.backlog.drop").functionCounter().count()).isEqualTo(5);
    assertThat(meters.get("gw.kernel.tcp.mem.pages").gauge().value()).isEqualTo(412);
  }

  @Test
  void notRegisteredWhenNetstatUnreadable() {
    assertThat(KernelStatsReader.forPaths(DIR.resolve("missing"), DIR.resolve("sockstat"))).isEmpty();
    assertThat(KernelStatsReader.forPaths(DIR.resolve("netstat"), DIR.resolve("sockstat"))).isPresent();
  }
}
