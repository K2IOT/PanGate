package com.pangate.gateway.lifecycle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.admin.NewTopic;

/** Kafka topic names and their partition counts (spec §2.4, §5.5, §6.8). */
public final class TopicNames {

  public static final String CONTROL = "gw.control";
  static final int LIFECYCLE_PARTITIONS = 32;
  static final int UPSTREAM_PARTITIONS = 64;
  static final int CONTROL_PARTITIONS = 8;

  private final String prefix;
  private final List<String> upstreamTypes;

  public TopicNames(String prefix, List<String> upstreamTypes) {
    this.prefix = prefix;
    this.upstreamTypes = List.copyOf(upstreamTypes);
  }

  public String upstream(String type) {
    return prefix + ".gw.upstream." + type;
  }

  public String lifecycle() {
    return prefix + ".gw.lifecycle";
  }

  public List<String> upstreamTypes() {
    return upstreamTypes;
  }

  public boolean isUpstreamType(String type) {
    return upstreamTypes.contains(type);
  }

  /** {@code cmd} and {@code cmd_result} go through the idempotent acks=all producer and are acknowledged. */
  public static boolean isReliable(String type) {
    return "cmd".equals(type) || "cmd_result".equals(type);
  }

  public List<NewTopic> all() {
    List<NewTopic> all = new ArrayList<>();
    all.add(new NewTopic(lifecycle(), Optional.of(LIFECYCLE_PARTITIONS), Optional.empty()));
    for (String t : upstreamTypes) {
      all.add(new NewTopic(upstream(t), Optional.of(UPSTREAM_PARTITIONS), Optional.empty()));
    }
    all.add(new NewTopic(CONTROL, Optional.of(CONTROL_PARTITIONS), Optional.empty())
        .configs(Map.of("cleanup.policy", "compact")));
    return all;
  }
}
