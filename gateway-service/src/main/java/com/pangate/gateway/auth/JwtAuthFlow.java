package com.pangate.gateway.auth;

import io.netty.channel.Channel;
import java.time.Duration;
import java.util.function.Consumer;

/**
 * Verifies a connect JWT for a channel without blocking its event loop (spec §4.6). On an unknown kid it pauses
 * reads, refreshes the JWKS off-loop (singleflight), then verifies again on the channel's loop. The callback runs
 * before reads resume, so frames the client pipelined after the JWT are handled after the result.
 */
public final class JwtAuthFlow {

  private final JwtVerifier verifier;
  private final JwksCache jwks;
  private final Duration refreshWait;

  public JwtAuthFlow(JwtVerifier verifier, JwksCache jwks, Duration refreshWait) {
    this.verifier = verifier;
    this.jwks = jwks;
    this.refreshWait = refreshWait;
  }

  public void verify(Channel ch, String jwt, long nowSec, Consumer<JwtResult> onLoop) {
    JwtResult first = verifier.verify(jwt, nowSec);
    if (!(first instanceof JwtResult.UnknownKid unknown)) {
      onLoop.accept(first);
      return;
    }
    ch.config().setAutoRead(false);
    jwks.refreshFor(unknown.kid()).orTimeout(refreshWait.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
        .whenComplete((found, err) -> ch.eventLoop().execute(() -> {
          JwtResult again = verifier.verify(jwt, nowSec);
          if (again instanceof JwtResult.UnknownKid) {
            again = new JwtResult.Rejected("unknown kid " + unknown.kid());
          }
          try {
            onLoop.accept(again);
          } catch (RuntimeException e) {
            // Never let a callback failure escape to the event loop: the pipeline's ErrorTerminator closes (4500).
            ch.pipeline().fireExceptionCaught(e);
          } finally {
            if (ch.isActive()) {
              ch.config().setAutoRead(true);
            }
          }
        }));
  }
}
