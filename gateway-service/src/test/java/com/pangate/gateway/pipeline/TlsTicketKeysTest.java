package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.pangate.gateway.dev.DevSecretsGenerator;
import com.pangate.starter.secrets.FileSecretSource;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.OpenSslSessionContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TlsTicketKeysTest {

  private static void writeKeys(Path d, byte seed) throws Exception {
    Files.createDirectories(d.resolve("tls-ticket-keys"));
    String[] roles = {"prev", "current", "next"};
    for (int i = 0; i < 3; i++) {
      byte[] k = new byte[48];
      java.util.Arrays.fill(k, (byte) (seed + i));
      Files.writeString(d.resolve("tls-ticket-keys").resolve(roles[i]), Base64.getEncoder().encodeToString(k) + "\n");
    }
  }

  @Test
  void loadsThreeKeysOf48BytesAndComparesByContent(@TempDir Path d) throws Exception {
    writeKeys(d, (byte) 1);
    TlsTicketKeys a = TlsTicketKeys.load(new FileSecretSource(d));
    TlsTicketKeys b = TlsTicketKeys.load(new FileSecretSource(d));
    assertThat(a).isEqualTo(b);
    writeKeys(d, (byte) 9);
    assertThat(TlsTicketKeys.load(new FileSecretSource(d))).isNotEqualTo(a);
  }

  @Test
  void wrongLengthNamesTheSecret(@TempDir Path d) throws Exception {
    writeKeys(d, (byte) 1);
    Files.writeString(d.resolve("tls-ticket-keys/current"), Base64.getEncoder().encodeToString(new byte[32]));
    assertThatThrownBy(() -> TlsTicketKeys.load(new FileSecretSource(d)))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("tls-ticket-keys/current");
  }

  @Test
  void ticketIssuedByOneContextResumesOnAnotherWithTheSameKeys(@TempDir Path d) throws Exception {
    assumeTrue(OpenSsl.isAvailable());
    DevSecretsGenerator.ensure(d);
    writeKeys(d, (byte) 1);
    TlsTicketKeys keys = TlsTicketKeys.load(new FileSecretSource(d));
    SslContext a = server(d);
    SslContext b = server(d);
    keys.applyTo(a);
    keys.applyTo(b);
    SslContext client = SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL)
        .trustManager(d.resolve("ca.crt").toFile()).protocols("TLSv1.3").build();
    MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    try {
      int port = roundTrip(group, a, client, 0);
      roundTrip(group, b, client, port);  // same host:port, so the client offers A's ticket to B
      assertThat(((OpenSslSessionContext) b.sessionContext()).stats().ticketKeyResume()).isEqualTo(1);
    } finally {
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }
  }

  @Test
  void differentKeysDoNotResume(@TempDir Path d, @TempDir Path other) throws Exception {
    assumeTrue(OpenSsl.isAvailable());
    DevSecretsGenerator.ensure(d);
    writeKeys(d, (byte) 1);
    writeKeys(other, (byte) 9);
    SslContext a = server(d);
    SslContext b = server(d);
    TlsTicketKeys.load(new FileSecretSource(d)).applyTo(a);
    TlsTicketKeys.load(new FileSecretSource(other)).applyTo(b);
    SslContext client = SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL)
        .trustManager(d.resolve("ca.crt").toFile()).protocols("TLSv1.3").build();
    MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    try {
      int port = roundTrip(group, a, client, 0);
      roundTrip(group, b, client, port);
      assertThat(((OpenSslSessionContext) b.sessionContext()).stats().ticketKeyResume()).isZero();
    } finally {
      group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }
  }

  /** Re-binding the previous port can race the close of the server side of the last connection: retry for 2 s. */
  private static Channel bind(ServerBootstrap sb, int port) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (true) {
      io.netty.channel.ChannelFuture f = sb.bind(new InetSocketAddress("127.0.0.1", port)).await();
      if (f.isSuccess() || System.nanoTime() > deadline) {
        return f.sync().channel();
      }
      Thread.sleep(50);
    }
  }

  @AfterEach
  void releaseServerContexts() {  // OPENSSL_REFCNT contexts hold native memory until released
    SERVERS.forEach(io.netty.util.ReferenceCountUtil::release);
    SERVERS.clear();
  }

  private static final java.util.List<SslContext> SERVERS = new java.util.ArrayList<>();

  private static SslContext server(Path d) throws Exception {
    SslContext ctx = buildServer(d);
    SERVERS.add(ctx);
    return ctx;
  }

  private static SslContext buildServer(Path d) throws Exception {
    return SslContextBuilder.forServer(d.resolve("gateway.crt").toFile(), d.resolve("gateway.key").toFile())
        .sslProvider(SslProvider.OPENSSL_REFCNT).protocols("TLSv1.3").build();
  }

  /** Starts an echo server on {@code port} (0 = any), sends one byte over TLS, waits for the echo; returns the port. */
  private static int roundTrip(MultiThreadIoEventLoopGroup group, SslContext server, SslContext client, int port)
      throws Exception {
    ServerBootstrap sb = new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
        .option(io.netty.channel.ChannelOption.SO_REUSEADDR, true)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) {
            ch.pipeline().addLast(server.newHandler(ch.alloc()), new SimpleChannelInboundHandler<ByteBuf>() {
              @Override
              protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                ctx.writeAndFlush(msg.retain());
              }
            });
          }
        });
    Channel listener = bind(sb, port);
    int bound = ((InetSocketAddress) listener.localAddress()).getPort();
    CompletableFuture<Void> echoed = new CompletableFuture<>();
    Channel ch = new Bootstrap().group(group).channel(NioSocketChannel.class)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel c) {
            c.pipeline().addLast(client.newHandler(c.alloc(), "localhost", bound),
                new SimpleChannelInboundHandler<ByteBuf>() {
                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                    echoed.complete(null);
                  }
                });
          }
        }).connect("127.0.0.1", bound).sync().channel();
    ch.writeAndFlush(Unpooled.wrappedBuffer(new byte[] {1}));
    echoed.get(5, TimeUnit.SECONDS);
    ch.close().sync();
    listener.close().sync();
    return bound;
  }
}
