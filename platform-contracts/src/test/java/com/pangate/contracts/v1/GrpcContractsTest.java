package com.pangate.contracts.v1;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.Test;

class GrpcContractsTest {

  @Test
  void pushStreamIsBidiStreaming() {
    assertThat(PushStreamGrpc.getStreamMethod().getFullMethodName()).isEqualTo("pangate.v1.PushStream/Stream");
    assertThat(PushStreamGrpc.getStreamMethod().getType()).isEqualTo(MethodDescriptor.MethodType.BIDI_STREAMING);
  }

  @Test
  void pushServiceIsBidiStreaming() {
    assertThat(PushServiceGrpc.getSendStreamMethod().getFullMethodName())
        .isEqualTo("pangate.v1.PushService/SendStream");
    assertThat(PushServiceGrpc.getSendStreamMethod().getType())
        .isEqualTo(MethodDescriptor.MethodType.BIDI_STREAMING);
  }
}
