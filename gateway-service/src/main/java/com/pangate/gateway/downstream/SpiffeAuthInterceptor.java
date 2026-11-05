package com.pangate.gateway.downstream;

import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

/** mTLS caller check (spec §6.3): the client certificate's URI SAN must be one of the allowed SPIFFE ids. */
public final class SpiffeAuthInterceptor implements ServerInterceptor {

  private static final int SAN_URI = 6;

  private final Set<String> allowed;

  public SpiffeAuthInterceptor(List<String> allowed) {
    this.allowed = Set.copyOf(allowed);
  }

  @Override
  public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
      ServerCallHandler<Q, R> next) {
    if (!authorized(call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION))) {
      call.close(Status.UNAUTHENTICATED.withDescription("client SPIFFE id not allowed"), new Metadata());
      return new ServerCall.Listener<>() { };
    }
    return next.startCall(call, headers);
  }

  private boolean authorized(SSLSession ssl) {
    if (ssl == null) {
      return false;
    }
    try {
      Certificate[] chain = ssl.getPeerCertificates();
      if (chain == null || chain.length == 0 || !(chain[0] instanceof X509Certificate peer)) {
        return false;
      }
      Collection<List<?>> sans = peer.getSubjectAlternativeNames();
      if (sans == null) {
        return false;
      }
      for (List<?> san : sans) {
        if (san.size() == 2 && Integer.valueOf(SAN_URI).equals(san.get(0)) && allowed.contains(san.get(1))) {
          return true;
        }
      }
      return false;
    } catch (SSLPeerUnverifiedException | CertificateParsingException e) {
      return false;
    }
  }
}
