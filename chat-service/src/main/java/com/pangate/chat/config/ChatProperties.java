package com.pangate.chat.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** {@code chat.*}, defaults of chat spec §4.3, §5, §8. */
@ConfigurationProperties("chat")
public record ChatProperties(
    @DefaultValue("c1") String cell,
    @DefaultValue("8081") int managementPort,
    @DefaultValue Kafka kafka,
    @DefaultValue Scylla scylla,
    @DefaultValue Redis redis,
    @DefaultValue Auth auth,
    @DefaultValue PushApi pushApi,
    @DefaultValue Limits limits,
    @DefaultValue Http http,
    @DefaultValue State state,
    @DefaultValue Notify notifications) {

  /** CH4 spec §3, §4: offline notifications. */
  public record Notify(@DefaultValue("true") boolean enabled,
      @DefaultValue("c1.push.offline") String offlineTopic,
      @DefaultValue("c1.notify.requests") String requestsTopic,
      @DefaultValue("1h") Duration maxAge,
      @DefaultValue("5000") int channelRate,
      @DefaultValue("1000") int channelPage) {}

  public record Kafka(@DefaultValue("kafka:9092") String bootstrap,
      @DefaultValue("chat.commands") String commandsTopic, @DefaultValue("chat.events") String eventsTopic,
      @DefaultValue("chat.commands.dlq") String dlqTopic, @DefaultValue("c1.gw.upstream.cmd") String upstreamTopic,
      @DefaultValue("5s") Duration produceTimeout,
      @DefaultValue("chat.receipts") String receiptsTopic) {}

  public record Scylla(@DefaultValue("scylla:9042") List<String> contactPoints,
      @DefaultValue("datacenter1") String localDc, @DefaultValue("chat") String keyspace,
      @DefaultValue("1s") Duration timeout, @DefaultValue("1") int replicationFactor) {}

  public record Redis(@DefaultValue("redis://redis:6379") String uri, @DefaultValue("200ms") Duration timeout,
      @DefaultValue("60s") Duration memberCacheTtl) {}

  public record Auth(@DefaultValue("https://auth.pangate/.well-known/jwks.json") String jwksUri,
      @DefaultValue("https://auth.pangate") String issuer, @DefaultValue("pangate-gw") String audience) {}

  public record PushApi(@DefaultValue("dns:///push-api:9443") String target, @DefaultValue("false") boolean plaintext,
      @DefaultValue("2s") Duration timeout) {}

  public record Limits(@DefaultValue("20") int userRate, @DefaultValue("40") int userBurst,
      @DefaultValue("50") int convRate, @DefaultValue("100") int convBurst,
      @DefaultValue("500") int maxGroupMembers, @DefaultValue("4096") int maxTextBytes,
      @DefaultValue("50") int maxMentions) {}

  public record Http(@DefaultValue("3s") Duration sendWait) {}

  /** Conversation state (CH3 spec §3.3, §3.4, §5, §6). */
  public record State(@DefaultValue("50") int markRate, @DefaultValue("100") int markBurst,
      @DefaultValue("500ms") Duration dmWindow, @DefaultValue("2s") Duration groupWindow,
      @DefaultValue("50") int listedReceiptsMax, @DefaultValue("5s") Duration offlineGrace,
      @DefaultValue("5m") Duration watchTtl, @DefaultValue("200") int watchMax,
      @DefaultValue("2s") Duration typingEvery, @DefaultValue("5") int typingRate,
      @DefaultValue("10") int typingBurst) {}
}
