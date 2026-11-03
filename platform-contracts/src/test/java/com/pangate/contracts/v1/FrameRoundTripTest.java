package com.pangate.contracts.v1;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class FrameRoundTripTest {

  @Test
  void helloRoundTripsInsideFramePayload() throws Exception {
    Hello hello = Hello.newBuilder().setJwt("t").setDeviceId("d1").setSdkVersion("1.0.0")
        .addCapabilities("zstd").build();
    Frame f = Frame.newBuilder().setType(FrameType.FRAME_TYPE_HELLO).setPayload(hello.toByteString()).build();

    Frame parsed = Frame.parseFrom(f.toByteArray());

    assertThat(parsed.getType()).isEqualTo(FrameType.FRAME_TYPE_HELLO);
    assertThat(Hello.parseFrom(parsed.getPayload())).isEqualTo(hello);
  }

  @Test
  void errorInfoCarriesFatalAndRetryAfter() throws Exception {
    ErrorInfo e = ErrorInfo.newBuilder().setCode(4013).setMessage("OVERLOADED").setRetryable(true)
        .setRetryAfterMs(15000).setFatal(true).build();

    ErrorInfo parsed = ErrorInfo.parseFrom(e.toByteArray());

    assertThat(parsed.getCode()).isEqualTo(4013);
    assertThat(parsed.getRetryAfterMs()).isEqualTo(15000);
    assertThat(parsed.getFatal()).isTrue();
  }

  @Test
  void connectedHasSeqUnknownField() throws Exception {
    Connected c = Connected.newBuilder().setConnId("01ABC").setHeartbeatSec(30).setSeqUnknown(true).build();

    Connected parsed = Connected.parseFrom(c.toByteArray());

    assertThat(parsed.getSeqUnknown()).isTrue();
    assertThat(parsed.getHeartbeatSec()).isEqualTo(30);
  }
}
