package com.pangate.devtools.client;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.Connected;
import com.pangate.contracts.v1.ErrorInfo;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Hello;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler.ClientHandshakeStateEvent;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Dev client for the gateway protocol over {@code wss://host:port/path} (WebSocket, subprotocol pangate.v1)
 * or {@code tcps://host:port} (TLS, ALPN pangate.v1, 4-byte length prefix). Blocking API; not thread-safe.
 */
public final class GatewayClient implements AutoCloseable {

  private static final EventLoopGroup GROUP =
      new MultiThreadIoEventLoopGroup(0, new DefaultThreadFactory("gw-client", true), NioIoHandler.newFactory());

  private final Channel channel;
  private final boolean websocket;
  private final LinkedTransferQueue<Frame> inbound;
  private final Inbound handler;

  private GatewayClient(Channel channel, boolean websocket, Inbound handler) {
    this.channel = channel;
    this.websocket = websocket;
    this.handler = handler;
    this.inbound = handler.queue;
  }

  public static GatewayClient connect(URI url, ClientTls tls, Duration timeout) {
    return connect(url, tls, timeout, null);
  }

  public static GatewayClient connect(URI url, ClientTls tls, Duration timeout, InetSocketAddress localBind) {
    boolean ws = switch (url.getScheme()) {
      case "wss" -> true;
      case "tcps" -> false;
      default -> throw new IllegalArgumentException("Unsupported scheme (use wss:// or tcps://): " + url);
    };
    String host = url.getHost();
    int port = url.getPort();
    Inbound handler = new Inbound(ws);
    io.netty.handler.ssl.SslContext sslContext = tls.context(!ws);
    Bootstrap b = new Bootstrap().group(GROUP).channel(NioSocketChannel.class)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) timeout.toMillis())
        .option(ChannelOption.TCP_NODELAY, true)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) {
            SslHandler ssl = sslContext.newHandler(ch.alloc(), host, port);
            ch.pipeline().addLast(ssl);
            if (ws) {
              ch.pipeline().addLast(new HttpClientCodec(), new HttpObjectAggregator(8192),
                  new WebSocketClientProtocolHandler(WebSocketClientProtocolConfig.newBuilder()
                      .webSocketUri(url).subprotocol("pangate.v1").handleCloseFrames(false)
                      .maxFramePayloadLength(65536).dropPongFrames(true).build()));
            } else {
              ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(66560, 0, 4, 0, 4), new LengthFieldPrepender(4));
            }
            ch.pipeline().addLast(new FrameEncoder(ws), handler);
          }
        });
    Channel ch;
    try {
      ch = (localBind == null ? b.connect(host, port) : b.connect(new InetSocketAddress(host, port), localBind))
          .sync().channel();
      handler.ready.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while connecting to " + url, e);
    } catch (Exception e) {
      Throwable cause = e instanceof java.util.concurrent.ExecutionException ? e.getCause() : e;
      throw new IllegalStateException("cannot connect to " + url + ": " + cause, cause);
    }
    return new GatewayClient(ch, ws, handler);
  }

  /** Sends a frame without waiting for anything. */
  public void send(Frame frame) {
    channel.writeAndFlush(frame);
  }

  /** Sends HELLO and waits for CONNECTED. */
  public Connected hello(Hello hello, Duration timeout) {
    send(Frame.newBuilder().setType(FrameType.FRAME_TYPE_HELLO).setPayload(hello.toByteString()).build());
    Frame f = await(FrameType.FRAME_TYPE_CONNECTED, timeout);
    try {
      return Connected.parseFrom(f.getPayload());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("malformed CONNECTED payload", e);
    }
  }

  /** Sends PING and waits for the matching PONG. */
  public Frame ping(Duration timeout) {
    String id = UUID.randomUUID().toString();
    send(Frame.newBuilder().setType(FrameType.FRAME_TYPE_PING).setMsgId(id).build());
    return await(FrameType.FRAME_TYPE_PONG, timeout);
  }

  /** Sends UPSTREAM {@code type} and waits for the gateway's answer: UPSTREAM_ACK (msg_id) or ERROR. */
  public Frame upstream(String type, String msgId, byte[] payload, Duration timeout) {
    send(Frame.newBuilder().setType(FrameType.FRAME_TYPE_UPSTREAM).setMsgId(msgId).putHeaders("type", type)
        .setPayload(com.google.protobuf.ByteString.copyFrom(payload)).build());
    return awaitFirst(f -> (f.getType() == FrameType.FRAME_TYPE_UPSTREAM_ACK && msgId.equals(f.getMsgId()))
        || f.getType() == FrameType.FRAME_TYPE_ERROR, "UPSTREAM_ACK or ERROR", timeout);
  }

  /** Waits for the next PUSH or RPC_REQ, discarding other frames. */
  public Frame awaitPush(Duration timeout) {
    return awaitFirst(f -> f.getType() == FrameType.FRAME_TYPE_PUSH || f.getType() == FrameType.FRAME_TYPE_RPC_REQ,
        "PUSH", timeout);
  }

  /** From now on PUSH and RPC_REQ frames go to {@code listener} (on the I/O thread) instead of the await queue. */
  public void onPush(java.util.function.Consumer<Frame> listener) {
    handler.pushListener = listener;
  }

  /** ACK for RELIABLE / ORDERED pushes (at most 32 ids per frame). */
  public void ack(String... msgIds) {
    send(Frame.newBuilder().setType(FrameType.FRAME_TYPE_ACK)
        .setPayload(com.pangate.contracts.v1.Ack.newBuilder().addAllMsgIds(java.util.List.of(msgIds)).build()
            .toByteString()).build());
  }

  /** SUBSCRIBE; the gateway answers only on rejection (ERROR 4000 with this frame's msg_id). Returns the msg_id. */
  public String subscribe(String... topics) {
    return topics(FrameType.FRAME_TYPE_SUBSCRIBE, topics);
  }

  public String unsubscribe(String... topics) {
    return topics(FrameType.FRAME_TYPE_UNSUBSCRIBE, topics);
  }

  private String topics(FrameType type, String... topics) {
    String id = UUID.randomUUID().toString();
    send(Frame.newBuilder().setType(type).setMsgId(id)
        .setPayload(com.pangate.contracts.v1.Subscribe.newBuilder().addAllTopics(java.util.List.of(topics)).build()
            .toByteString()).build());
    return id;
  }

  /** Waits for the next frame of {@code type}, discarding other frames. */
  public Frame await(FrameType type, Duration timeout) {
    return awaitFirst(f -> f.getType() == type, type.name(), timeout);
  }

  private Frame awaitFirst(java.util.function.Predicate<Frame> match, String what, Duration timeout) {
    long deadline = System.nanoTime() + timeout.toNanos();
    try {
      while (true) {
        long left = deadline - System.nanoTime();
        if (left <= 0) {
          throw new IllegalStateException(new TimeoutException("no " + what + " within " + timeout));
        }
        Frame f = inbound.poll(Math.min(left, TimeUnit.MILLISECONDS.toNanos(50)), TimeUnit.NANOSECONDS);
        if (f != null) {
          if (match.test(f)) {
            return f;
          }
          continue;
        }
        if (!channel.isActive() && inbound.isEmpty()) {
          throw new GatewayClosedException(handler.closeInfo());
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  /** Waits until the connection is closed and returns how. */
  public CloseInfo awaitClose(Duration timeout) {
    if (!channel.closeFuture().awaitUninterruptibly(timeout.toMillis())) {
      throw new IllegalStateException(new TimeoutException("connection still open after " + timeout));
    }
    return handler.closeInfo();
  }

  public boolean isOpen() {
    return channel.isActive();
  }

  /** Normal close: WS close frame 1000, or FIN on TCP. */
  @Override
  public void close() {
    if (!channel.isActive()) {
      return;
    }
    if (websocket) {
      channel.writeAndFlush(new CloseWebSocketFrame(1000, "bye")).addListener(ChannelFutureListener.CLOSE);
    } else {
      channel.close();
    }
    channel.closeFuture().awaitUninterruptibly(2000);
  }

  /** Abrupt close: TCP RST, no close frame. */
  public void abort() {
    channel.config().setOption(ChannelOption.SO_LINGER, 0);
    channel.close().awaitUninterruptibly(2000);
  }

  private static final class FrameEncoder extends MessageToMessageEncoder<Frame> {
    private final boolean websocket;

    FrameEncoder(boolean websocket) {
      this.websocket = websocket;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Frame msg, List<Object> out) {
      ByteBuf buf = Unpooled.wrappedBuffer(msg.toByteArray());
      out.add(websocket ? new BinaryWebSocketFrame(buf) : buf);
    }
  }

  private static final class Inbound extends SimpleChannelInboundHandler<Object> {
    final LinkedTransferQueue<Frame> queue = new LinkedTransferQueue<>();
    final CompletableFuture<Void> ready = new CompletableFuture<>();
    private final boolean websocket;
    private volatile CloseInfo closeInfo;
    volatile java.util.function.Consumer<Frame> pushListener;

    Inbound(boolean websocket) {
      this.websocket = websocket;
    }

    CloseInfo closeInfo() {
      return closeInfo == null ? CloseInfo.ABNORMAL : closeInfo;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (websocket && evt == ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
        ready.complete(null);
      } else if (!websocket && evt instanceof SslHandshakeCompletionEvent e) {
        if (!e.isSuccess()) {
          ready.completeExceptionally(e.cause());
        } else if (!ClientTls.ALPN_PANGATE.equals(ctx.pipeline().get(SslHandler.class).applicationProtocol())) {
          ready.completeExceptionally(new IllegalStateException("server did not select ALPN pangate.v1"));
          ctx.close();
        } else {
          ready.complete(null);
        }
      }
      super.userEventTriggered(ctx, evt);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (msg instanceof CloseWebSocketFrame close) {
        closeInfo = new CloseInfo(close.statusCode(), close.reasonText());
        ctx.close();
        return;
      }
      ByteBuf content = msg instanceof WebSocketFrame wsf ? wsf.content() : (ByteBuf) msg;
      if (msg instanceof WebSocketFrame && !(msg instanceof BinaryWebSocketFrame)) {
        return;
      }
      Frame f = Frame.parseFrom(ByteBufUtil.getBytes(content));
      if (f.getType() == FrameType.FRAME_TYPE_ERROR) {
        ErrorInfo err = ErrorInfo.parseFrom(f.getPayload());
        if (err.getFatal()) {
          closeInfo = new CloseInfo(err.getCode(), err.getMessage());
        }
      }
      java.util.function.Consumer<Frame> l = pushListener;
      if (l != null && (f.getType() == FrameType.FRAME_TYPE_PUSH || f.getType() == FrameType.FRAME_TYPE_RPC_REQ)) {
        l.accept(f);
        return;
      }
      queue.add(f);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      ready.completeExceptionally(new IllegalStateException("connection closed during handshake"));
      super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      ready.completeExceptionally(cause);
      ctx.close();
    }
  }
}
