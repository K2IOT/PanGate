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

  /**
   * Perf 3b (spec §4.5): {@code streams} per pod over round-robin; per stream at most {@code window} unanswered
   * requests (≤ the Push API's max-in-flight-per-stream), of which BEST_EFFORT + DROPPABLE use at most
   * {@code lowShare}; BEST_EFFORT waits at most {@code lowWait}, MUST at most {@code mustWait}; {@code queueMax} per
   * stream and queue; {@code timeout} counts from the write; each call is rotated after {@code streamMaxAge} (±20%),
   * below the Push API's max-connection-age grace, so its connection age never cuts a call with requests on it.
   */
  public record PushApi(@DefaultValue("dns:///push-api:9443") String target, @DefaultValue("false") boolean plaintext,
      @DefaultValue("8s") Duration timeout, @DefaultValue("4") int streams, @DefaultValue("1000") int window,
      @DefaultValue("0.2") double lowShare, @DefaultValue("2s") Duration lowWait,
      @DefaultValue("10000") int queueMax, @DefaultValue("10s") Duration mustWait,
      @DefaultValue("5m") Duration streamMaxAge) {}

  public record Limits(@DefaultValue("20") int userRate, @DefaultValue("40") int userBurst,
      @DefaultValue("50") int convRate, @DefaultValue("100") int convBurst,
      @DefaultValue("500") int maxGroupMembers, @DefaultValue("4096") int maxTextBytes,
      @DefaultValue("50") int maxMentions, @DefaultValue Api api) {}

  /** Perf 3a (spec §3.2): per-user limits of the HTTP API (rate per second, burst), per pod. */
  public record Api(@DefaultValue("5") int listRate, @DefaultValue("20") int listBurst,
      @DefaultValue("2") int stateRate, @DefaultValue("10") int stateBurst,
      @DefaultValue("2") int syncRate, @DefaultValue("10") int syncBurst,
      @DefaultValue("2") int presenceRate, @DefaultValue("10") int presenceBurst,
      @DefaultValue("20") int historyRate, @DefaultValue("50") int historyBurst,
      @DefaultValue("5") int manageRate, @DefaultValue("20") int manageBurst,
      @DefaultValue("10") int otherRate, @DefaultValue("30") int otherBurst) {
    public static final Api DEFAULTS = new Api(5, 20, 2, 10, 2, 10, 2, 10, 20, 50, 5, 20, 10, 30);
  }

  public record Http(@DefaultValue("3s") Duration sendWait) {}

  /** Conversation state (CH3 spec §3.3, §3.4, §5, §6). */
  public record State(@DefaultValue("50") int markRate, @DefaultValue("100") int markBurst,
      @DefaultValue("500ms") Duration dmWindow, @DefaultValue("2s") Duration groupWindow,
      @DefaultValue("50") int listedReceiptsMax, @DefaultValue("5s") Duration offlineGrace,
      @DefaultValue("5m") Duration watchTtl, @DefaultValue("200") int watchMax,
      @DefaultValue("2s") Duration typingEvery, @DefaultValue("5") int typingRate,
      @DefaultValue("10") int typingBurst, @DefaultValue("1") int watchRate, @DefaultValue("5") int watchBurst) {}
}
