package com.pangate.gateway.test;

import com.pangate.gateway.config.GatewayLimits;
import com.pangate.gateway.config.LimitsHolder;
import java.time.Duration;

/** Production limit values (spec §9) for unit tests. */
public final class TestLimits {

  private TestLimits() {}

  public static GatewayLimits defaults() {
    return new GatewayLimits(700_000, 550_000, 200, 65_536, 8_192, Duration.ofSeconds(5), Duration.ofSeconds(10),
        20, Duration.ofSeconds(30), Duration.ofHours(24), 50, 100, 100, 32, 65_536, Duration.ofSeconds(5), 10_000, Duration.ofSeconds(30));
  }

  public static GatewayLimits withBroadcastRate(int perLoop) {
    GatewayLimits d = defaults();
    return new GatewayLimits(d.hardCap(), d.softCap(), d.perIp(), d.frameMax(), d.helloMax(), d.helloTimeout(),
        d.handshakeTimeout(), d.handshakesPerPrincipalPerMin(), d.heartbeatInterval(), d.sessionTtl(),
        d.inboundRate(), d.inboundBurst(), d.topicsPerConn(), d.pendingAckMsgs(), d.pendingAckBytes(),
        d.pendingAckTimeout(), perLoop, d.slowConsumerTimeout());
  }

  public static LimitsHolder holder() {
    return new LimitsHolder(defaults());
  }
}
