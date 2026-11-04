package com.pangate.gateway.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Kernel listen-queue and TCP memory counters (spec §8): {@code ListenOverflows}, {@code ListenDrops},
 * {@code TCPBacklogDrop} from /proc/net/netstat (TcpExt), TCP memory pages from /proc/net/sockstat. Registered only
 * on Linux with a readable netstat. {@link #refresh()} reads files: call it off the event loops and off gw-tick.
 */
public final class KernelStatsReader {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(KernelStatsReader.class);

  private final Path netstat;
  private final Path sockstat;
  private volatile Map<String, Long> tcpExt = Map.of();
  private volatile long tcpMemPages;

  public KernelStatsReader(Path netstat, Path sockstat) {
    this.netstat = netstat;
    this.sockstat = sockstat;
  }

  public static Optional<KernelStatsReader> forThisHost() {
    if (!System.getProperty("os.name", "").toLowerCase().contains("linux")) {
      log.info("kernel stats not collected: not Linux");
      return Optional.empty();
    }
    return forPaths(Path.of("/proc/net/netstat"), Path.of("/proc/net/sockstat"));
  }

  static Optional<KernelStatsReader> forPaths(Path netstat, Path sockstat) {
    if (!Files.isReadable(netstat)) {
      log.info("kernel stats not collected: {} is not readable", netstat);
      return Optional.empty();
    }
    return Optional.of(new KernelStatsReader(netstat, sockstat));
  }

  public void refresh() {
    tcpExt = parseTcpExt(netstat);
    tcpMemPages = parseTcpMem(sockstat);
  }

  public long tcpExt(String field) {
    return tcpExt.getOrDefault(field, 0L);
  }

  public long tcpMemPages() {
    return tcpMemPages;
  }

  public void bindTo(MeterRegistry r) {
    Gauge.builder("gw.kernel.listen.overflows", this, k -> k.tcpExt("ListenOverflows")).register(r);
    Gauge.builder("gw.kernel.listen.drops", this, k -> k.tcpExt("ListenDrops")).register(r);
    Gauge.builder("gw.kernel.tcp.backlog.drop", this, k -> k.tcpExt("TCPBacklogDrop")).register(r);
    Gauge.builder("gw.kernel.tcp.mem.pages", this, KernelStatsReader::tcpMemPages).register(r);
  }

  /** Header line "TcpExt: names…" followed by the line "TcpExt: values…" in the same column order. */
  private static Map<String, Long> parseTcpExt(Path p) {
    List<String> lines = read(p);
    Map<String, Long> out = new HashMap<>();
    for (int i = 0; i + 1 < lines.size(); i++) {
      if (lines.get(i).startsWith("TcpExt:") && lines.get(i + 1).startsWith("TcpExt:")) {
        String[] names = lines.get(i).trim().split("\\s+");
        String[] values = lines.get(i + 1).trim().split("\\s+");
        for (int c = 1; c < Math.min(names.length, values.length); c++) {
          try {
            out.put(names[c], Long.parseLong(values[c]));
          } catch (NumberFormatException ignored) {
            // skip a malformed column
          }
        }
        break;
      }
    }
    return out;
  }

  /** "TCP: inuse N orphan N tw N alloc N mem N" → mem. */
  private static long parseTcpMem(Path p) {
    for (String line : read(p)) {
      if (line.startsWith("TCP:")) {
        String[] f = line.trim().split("\\s+");
        for (int i = 1; i + 1 < f.length; i++) {
          if ("mem".equals(f[i])) {
            try {
              return Long.parseLong(f[i + 1]);
            } catch (NumberFormatException e) {
              return 0;
            }
          }
        }
      }
    }
    return 0;
  }

  private static List<String> read(Path p) {
    try {
      return Files.readAllLines(p);
    } catch (IOException | RuntimeException e) {
      return List.of();
    }
  }
}
