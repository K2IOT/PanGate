package com.pangate.gateway.lifecycle;

import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Hello;
import com.pangate.gateway.auth.WarmUpTokens;
import com.pangate.gateway.config.GatewayProperties;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import javax.net.ssl.SSLException;

/**
 * Warm-up before readiness (spec §7.6): {@code iterations} loopback connections, {@code concurrency} at a time,
 * each TLS → WS upgrade → HELLO (warm-up token) → CONNECTED → PING → PONG → close, so TLS, the codecs and the
 * HELLO path are JIT-compiled before real clients arrive. Bounded by {@code timeout}; never fails startup.
 * Trusts any certificate: it only ever connects to 127.0.0.1.
 */
public final class WarmUpRunner {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(WarmUpRunner.class);

  public record Result(int ok, int failed, boolean timedOut) {}

  private final GatewayProperties.Warmup cfg;
  private final String path;
  private final WarmUpTokens tokens;
  private final LongSupplier wallSec;

  public WarmUpRunner(GatewayProperties.Warmup cfg, String wsPath, WarmUpTokens tokens, LongSupplier wallSec) {
    this.cfg = cfg;
    this.path = wsPath;
    this.tokens = tokens;
    this.wallSec = wallSec;
  }

  /** {@code port}: the WS listener's bound port (not the configured one, which may be 0). */
  public Result run(int port) {
    if (cfg.iterations() <= 0) {
      return new Result(0, 0, false);
    }
    long t0 = System.nanoTime();
    long deadline = t0 + cfg.timeout().toNanos();
    MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    AtomicInteger ok = new AtomicInteger();
    AtomicInteger failed = new AtomicInteger();
    java.util.concurrent.atomic.AtomicReference<Throwable> firstFailure = new java.util.concurrent.atomic.AtomicReference<>();
    boolean timedOut = false;
    try {
      SslContext ssl = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
      Semaphore slots = new Semaphore(Math.max(1, cfg.concurrency()));
      for (int i = 0; i < cfg.iterations(); i++) {
        long left = deadline - System.nanoTime();
        if (left <= 0 || !slots.tryAcquire(left, TimeUnit.NANOSECONDS)) {
          timedOut = true;
          break;
        }
        once(group, ssl, port, i).orTimeout(Math.max(1, left / 1_000_000), TimeUnit.MILLISECONDS)
            .whenComplete((v, e) -> {
              (e == null ? ok : failed).incrementAndGet();
              if (e != null) {
                firstFailure.compareAndSet(null, e);
              }
              slots.release();
            });
      }
      long left = deadline - System.nanoTime();
      int all = Math.max(1, cfg.concurrency());
      if (left <= 0 || !slots.tryAcquire(all, left, TimeUnit.NANOSECONDS)) {
        timedOut = true;
      }
    } catch (SSLException e) {
      log.warn("warm-up skipped: {}", e.toString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      timedOut = true;
    } finally {
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }
    Result r = new Result(ok.get(), failed.get(), timedOut);
    long tookMs = (System.nanoTime() - t0) / 1_000_000;
    if (timedOut || r.failed() > 0) {
      log.warn("warm-up: ok={}, failed={}, timedOut={}, took={} ms, first failure: {}", r.ok(), r.failed(), timedOut,
          tookMs, String.valueOf(firstFailure.get()));
    } else {
      log.info("warm-up: ok={}, took={} ms", r.ok(), tookMs);
    }
    return r;
  }

  private CompletableFuture<Void> once(MultiThreadIoEventLoopGroup group, SslContext ssl, int port, int i) {
    CompletableFuture<Void> done = new CompletableFuture<>();
    // Minted here, not on the client's event loop: the jti comes from SecureRandom (a blocking read).
    String jwt = tokens.mint(i, wallSec.getAsLong());
    URI uri = URI.create("wss://127.0.0.1:" + port + path);
    WebSocketClientProtocolConfig ws = WebSocketClientProtocolConfig.newBuilder().webSocketUri(uri)
        .subprotocol("pangate.v1").handleCloseFrames(false).build();
    new Bootstrap().group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) {
        ch.pipeline().addLast(ssl.newHandler(ch.alloc(), "127.0.0.1", port), new HttpClientCodec(),
            new HttpObjectAggregator(8192), new WebSocketClientProtocolHandler(ws), new Exchange(i, jwt, done));
      }
    }).connect("127.0.0.1", port).addListener((ChannelFutureListener) f -> {
      if (!f.isSuccess()) {
        done.completeExceptionally(f.cause());
      } else {
        f.channel().closeFuture().addListener(c -> done.completeExceptionally(new IllegalStateException("closed")));
      }
    });
    return done;
  }

  /** HELLO after the upgrade, PING after CONNECTED, close after PONG. */
  private final class Exchange extends SimpleChannelInboundHandler<WebSocketFrame> {
    private final int i;
    private final String jwt;
    private final CompletableFuture<Void> done;

    Exchange(int i, String jwt, CompletableFuture<Void> done) {
      this.i = i;
      this.jwt = jwt;
      this.done = done;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
        Hello hello = Hello.newBuilder().setJwt(jwt).setDeviceId("warmup-" + i)
            .setSdkVersion("warmup").setPlatform("warmup").build();
        send(ctx, Frame.newBuilder().setType(FrameType.FRAME_TYPE_HELLO).setPayload(hello.toByteString()).build());
      }
      super.userEventTriggered(ctx, evt);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame msg) throws Exception {
      if (!(msg instanceof BinaryWebSocketFrame)) {
        ctx.close();
        return;
      }
      Frame f = Frame.parseFrom(ByteBufUtil.getBytes(msg.content()));
      switch (f.getType()) {
        case FRAME_TYPE_CONNECTED -> send(ctx, Frame.newBuilder().setType(FrameType.FRAME_TYPE_PING).build());
        case FRAME_TYPE_PONG -> {
          done.complete(null);
          ctx.writeAndFlush(new CloseWebSocketFrame(1000, "warm-up")).addListener(ChannelFutureListener.CLOSE);
        }
        default -> {
          done.completeExceptionally(new IllegalStateException("unexpected " + f.getType()));
          ctx.close();
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      done.completeExceptionally(cause);
      ctx.close();
    }

    private void send(ChannelHandlerContext ctx, Frame f) {
      ctx.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(f.toByteArray())));
    }
  }
}
