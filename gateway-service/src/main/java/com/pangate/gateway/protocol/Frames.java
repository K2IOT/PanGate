package com.pangate.gateway.protocol;

import com.pangate.contracts.v1.Connected;
import com.pangate.contracts.v1.ErrorInfo;
import com.pangate.contracts.v1.Frame;
import com.pangate.contracts.v1.FrameType;
import com.pangate.contracts.v1.Reauth;
import com.pangate.contracts.v1.Reconnect;

/** Gateway-to-client frame factories. */
public final class Frames {

  private Frames() {}

  public static Frame pong(String msgId) {
    return Frame.newBuilder().setType(FrameType.FRAME_TYPE_PONG).setMsgId(msgId).build();
  }

  /** Kafka accepted the client's cmd / cmd_result (spec §5.5 step 6). */
  public static Frame upstreamAck(String msgId) {
    return Frame.newBuilder().setType(FrameType.FRAME_TYPE_UPSTREAM_ACK).setMsgId(msgId).build();
  }

  public static Frame connected(Connected c) {
    return Frame.newBuilder().setType(FrameType.FRAME_TYPE_CONNECTED).setPayload(c.toByteString()).build();
  }

  public static Frame reauth(Reauth r) {
    return Frame.newBuilder().setType(FrameType.FRAME_TYPE_REAUTH).setPayload(r.toByteString()).build();
  }

  public static Frame reconnect(Reconnect r) {
    return Frame.newBuilder().setType(FrameType.FRAME_TYPE_RECONNECT).setPayload(r.toByteString()).build();
  }

  public static Frame error(int code, String message, boolean retryable, boolean fatal) {
    return error(code, message, retryable, fatal, 0);
  }

  public static Frame error(int code, String message, boolean retryable, boolean fatal, long retryAfterMs) {
    ErrorInfo e = ErrorInfo.newBuilder().setCode(code).setMessage(message).setRetryable(retryable)
        .setFatal(fatal).setRetryAfterMs((int) retryAfterMs).build();
    return Frame.newBuilder().setType(FrameType.FRAME_TYPE_ERROR).setPayload(e.toByteString()).build();
  }
}
