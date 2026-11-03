package com.pangate.gateway.pipeline;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.ErrorInfo;
import com.pangate.contracts.v1.Frame;
import com.pangate.gateway.protocol.ChannelAttrs;
import com.pangate.gateway.protocol.CloseCode;
import com.pangate.gateway.protocol.ProtocolException;
import com.pangate.gateway.session.Session;
import com.pangate.gateway.test.LoopFixture;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.websocketx.CorruptedWebSocketFrameException;
import io.netty.handler.ssl.NotSslRecordException;
import java.net.InetAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ErrorTerminatorTest {

  private final LoopFixture f = new LoopFixture();
  private Session session;

  @BeforeEach
  void setUp() {
    f.channel.attr(ChannelAttrs.TRANSPORT).set(ChannelAttrs.TransportKind.TCP);
    f.channel.attr(ChannelAttrs.PROTOCOL_READY).set(true);
    session = f.tracker.admit(f.channel, f.loop, InetAddress.getLoopbackAddress());
    f.channel.pipeline().addLast(new ErrorTerminator());
  }

  private int closeCodeSent() throws Exception {
    f.channel.runPendingTasks();
    Frame out = f.channel.readOutbound();
    return ErrorInfo.parseFrom(out.getPayload()).getCode();
  }

  @Test
  void tooLongFrameIsBadFrame() throws Exception {
    f.channel.pipeline().fireExceptionCaught(new TooLongFrameException("too long"));

    assertThat(closeCodeSent()).isEqualTo(4000);
  }

  @Test
  void corruptedWebSocketFrameIsBadFrame() throws Exception {
    f.channel.pipeline().fireExceptionCaught(new CorruptedWebSocketFrameException());

    assertThat(closeCodeSent()).isEqualTo(4000);
  }

  @Test
  void tlsGarbageClosesSilently() {
    f.channel.pipeline().fireExceptionCaught(new DecoderException(new NotSslRecordException("plaintext")));
    f.channel.runPendingTasks();

    assertThat(f.channel.isOpen()).isFalse();
    assertThat((Object) f.channel.readOutbound()).isNull();
  }

  @Test
  void protocolViolationIsRecordedOnTheSession() {
    f.channel.pipeline().fireExceptionCaught(new ProtocolException(CloseCode.BAD_FRAME, "bad"));
    f.channel.runPendingTasks();

    assertThat(session.closeCode()).isEqualTo(CloseCode.BAD_FRAME);
    assertThat(f.meters.get("gw.closes").tag("code", "4000").counter().count()).isEqualTo(1);
  }

  @Test
  void unexpectedErrorIsRecordedAsInternal() {
    f.channel.pipeline().fireExceptionCaught(new IllegalStateException("boom"));
    f.channel.runPendingTasks();

    assertThat(session.closeCode()).isEqualTo(CloseCode.INTERNAL);
  }
}
