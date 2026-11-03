package com.pangate.devtools.client;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.Connected;
import com.pangate.contracts.v1.ErrorInfo;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Hello;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import java.net.InetSocketAddress;

/** Minimal test double of the gateway: WSS and TCP+TLS listeners that answer HELLO and PING. */
final class FakeGatewayServer implements AutoCloseable {

  final SelfSignedCertificate cert;
  private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
  private final Channel ws;
  private final Channel tcp;

  FakeGatewayServer() throws Exception {
    cert = new SelfSignedCertificate("localhost");
    SslContext wsCtx = SslContextBuilder.forServer(cert.certificate(), cert.privateKey()).build();
    SslContext tcpCtx = SslContextBuilder.forServer(cert.certificate(), cert.privateKey())
        .applicationProtocolConfig(new ApplicationProtocolConfig(Protocol.ALPN,
            SelectorFailureBehavior.NO_ADVERTISE, SelectedListenerFailureBehavior.ACCEPT, "pangate.v1"))
        .build();
    ws = new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) {
            ch.pipeline().addLast(wsCtx.newHandler(ch.alloc()), new HttpServerCodec(), new HttpObjectAggregator(8192),
                new WebSocketServerProtocolHandler("/v1/ws", "pangate.v1"),
                new SimpleChannelInboundHandler<BinaryWebSocketFrame>() {
                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, BinaryWebSocketFrame msg) throws Exception {
                    handle(ctx.channel(), msg.content(), true);
                  }
                });
          }
        }).bind(new InetSocketAddress(0)).sync().channel();
    tcp = new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) {
            ch.pipeline().addLast(tcpCtx.newHandler(ch.alloc()), new LengthFieldBasedFrameDecoder(66560, 0, 4, 0, 4),
                new LengthFieldPrepender(4), new SimpleChannelInboundHandler<ByteBuf>() {
                  @Override
                  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
                    handle(ctx.channel(), msg, false);
                  }
                });
          }
        }).bind(new InetSocketAddress(0)).sync().channel();
  }

  int wsPort() {
    return ((InetSocketAddress) ws.localAddress()).getPort();
  }

  int tcpPort() {
    return ((InetSocketAddress) tcp.localAddress()).getPort();
  }

  private static void handle(Channel ch, ByteBuf bytes, boolean websocket) throws InvalidProtocolBufferException {
    byte[] arr = new byte[bytes.readableBytes()];
    bytes.readBytes(arr);
    Frame in = Frame.parseFrom(arr);
    if (in.getType() == FrameType.FRAME_TYPE_HELLO) {
      Hello hello = Hello.parseFrom(in.getPayload());
      if ("kick".equals(hello.getDeviceId())) {
        if (websocket) {
          ch.writeAndFlush(new CloseWebSocketFrame(4012, "DUPLICATE_SESSION")).addListener(ChannelFutureListener.CLOSE);
        } else {
          Frame err = Frame.newBuilder().setType(FrameType.FRAME_TYPE_ERROR).setPayload(ErrorInfo.newBuilder()
              .setCode(4012).setMessage("DUPLICATE_SESSION").setFatal(true).build().toByteString()).build();
          write(ch, err, false).addListener(ChannelFutureListener.CLOSE);
        }
        return;
      }
      write(ch, Frame.newBuilder().setType(FrameType.FRAME_TYPE_CONNECTED)
          .setPayload(Connected.newBuilder().setConnId("01TEST").setHeartbeatSec(30).build().toByteString()).build(),
          websocket);
    } else if (in.getType() == FrameType.FRAME_TYPE_PING) {
      write(ch, Frame.newBuilder().setType(FrameType.FRAME_TYPE_PONG).setMsgId(in.getMsgId()).build(), websocket);
    }
  }

  private static io.netty.channel.ChannelFuture write(Channel ch, Frame f, boolean websocket) {
    ByteBuf buf = Unpooled.wrappedBuffer(f.toByteArray());
    return ch.writeAndFlush(websocket ? new BinaryWebSocketFrame(buf) : buf);
  }

  @Override
  public void close() {
    ws.close();
    tcp.close();
    group.shutdownGracefully();
    cert.delete();
  }
}
