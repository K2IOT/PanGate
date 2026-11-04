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

  /** The real client address from a PROXY header (spec §4.1); absent for direct connections. */
  public static final AttributeKey<java.net.InetSocketAddress> CLIENT_ADDRESS =
      AttributeKey.valueOf("pangate.clientAddress");
  /** Test seam: the socket peer of a channel whose remote address is not an InetSocketAddress (EmbeddedChannel). */
  public static final AttributeKey<java.net.InetAddress> SOCKET_PEER_OVERRIDE =
      AttributeKey.valueOf("pangate.socketPeerOverride");

  private ChannelAttrs() {}

  /** The TCP peer of the socket itself (never a PROXY-header claim): used for loopback-only checks. */
  public static java.net.InetAddress socketPeer(io.netty.channel.Channel ch) {
    if (ch.remoteAddress() instanceof java.net.InetSocketAddress isa && isa.getAddress() != null) {
      return isa.getAddress();
    }
    java.net.InetAddress override = ch.attr(SOCKET_PEER_OVERRIDE).get();
    return override != null ? override : java.net.InetAddress.getLoopbackAddress();
  }

  /** The client's address for admission, per-IP limits and logs: the PROXY header's source if any, else the socket. */
  public static java.net.InetAddress clientAddress(io.netty.channel.Channel ch) {
    java.net.InetSocketAddress c = ch.attr(CLIENT_ADDRESS).get();
    return c != null && c.getAddress() != null ? c.getAddress() : socketPeer(ch);
  }

  public static boolean isSet(io.netty.channel.Channel ch, AttributeKey<Boolean> key) {
    return Boolean.TRUE.equals(ch.attr(key).get());
  }
}
