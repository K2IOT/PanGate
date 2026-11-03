package com.pangate.gateway.protocol;

import io.netty.util.AttributeKey;

/** Per-channel flags shared by the pipeline handlers. */
public final class ChannelAttrs {

  public enum TransportKind { WS, TCP }

  public static final AttributeKey<TransportKind> TRANSPORT = AttributeKey.valueOf("pangate.transport");
  /** Application frames can flow: WS upgraded, or TLS + ALPN done on TCP. */
  public static final AttributeKey<Boolean> PROTOCOL_READY = AttributeKey.valueOf("pangate.protocolReady");
  public static final AttributeKey<Boolean> AUTHENTICATED = AttributeKey.valueOf("pangate.authenticated");
  public static final AttributeKey<Boolean> CLOSING = AttributeKey.valueOf("pangate.closing");
  /** Close code a WS client sent in its close frame. */
  public static final AttributeKey<Integer> CLIENT_CLOSE_CODE = AttributeKey.valueOf("pangate.clientCloseCode");

  private ChannelAttrs() {}

  public static boolean isSet(io.netty.channel.Channel ch, AttributeKey<Boolean> key) {
    return Boolean.TRUE.equals(ch.attr(key).get());
  }
}
