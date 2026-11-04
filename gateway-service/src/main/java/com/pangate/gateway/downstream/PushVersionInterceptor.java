package com.pangate.gateway.downstream;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

/** Accepts push protocol versions {N, N−1} ≥ 1 in header {@code pangate-push-version} (spec §6.3); now N = 1. */
public final class PushVersionInterceptor implements ServerInterceptor {

  public static final int CURRENT = 1;
  public static final Metadata.Key<String> HEADER =
      Metadata.Key.of("pangate-push-version", Metadata.ASCII_STRING_MARSHALLER);

  static boolean accepted(String v) {
    if (v == null) {
      return false;
    }
    try {
      int n = Integer.parseInt(v.trim());
      return n >= 1 && (n == CURRENT || n == CURRENT - 1);
    } catch (NumberFormatException e) {
      return false;
    }
  }

  @Override
  public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
      ServerCallHandler<Q, R> next) {
    if (!accepted(headers.get(HEADER))) {
      call.close(Status.FAILED_PRECONDITION.withDescription("unsupported pangate-push-version"), new Metadata());
      return new ServerCall.Listener<>() { };
    }
    return next.startCall(call, headers);
  }
}
