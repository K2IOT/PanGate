package com.pangate.gateway.diagnostics;

import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.VMOption;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.internal.PlatformDependent;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** JVM memory and GC settings the gateway depends on; logged at start and checked per profile (spec §9.1). */
public record JvmSettings(List<String> gcNames, long maxHeapBytes, long maxDirectBytes, boolean directExplicit,
    String leakLevel) {

  private static final long MIB = 1024 * 1024;

  public JvmSettings {
    gcNames = List.copyOf(gcNames);
  }

  public static JvmSettings read() {
    List<String> gcs = ManagementFactory.getGarbageCollectorMXBeans().stream()
        .map(GarbageCollectorMXBean::getName).toList();
    boolean explicit = false;
    try {
      VMOption o = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
          .getVMOption("MaxDirectMemorySize");
      explicit = o.getOrigin() != VMOption.Origin.DEFAULT;
    } catch (RuntimeException e) {
      // not HotSpot: treat as unset
    }
    return new JvmSettings(gcs, Runtime.getRuntime().maxMemory(), PlatformDependent.maxDirectMemory(), explicit,
        ResourceLeakDetector.getLevel().name().toLowerCase(Locale.ROOT));
  }

  /** Profile prod needs an explicit direct-memory cap: the default (= max heap) plus the heap fills the container. */
  public void check(Set<String> activeProfiles) {
    if (activeProfiles.contains("prod") && !directExplicit) {
      throw new JvmConfigException("MaxDirectMemorySize");
    }
  }

  public String describe() {
    return "jvm gc=" + String.join(",", gcNames) + " heapMax=" + maxHeapBytes / MIB + "MiB directMax="
        + (directExplicit ? maxDirectBytes / MIB + "MiB" : "default") + " leak=" + leakLevel;
  }
}
