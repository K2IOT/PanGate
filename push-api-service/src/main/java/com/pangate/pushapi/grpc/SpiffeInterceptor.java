package com.pangate.pushapi.grpc;

import io.grpc.Context;
import io.grpc.Contexts;
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
import java.util.Optional;
import java.util.Set;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

/**
 * mTLS caller check (push-api spec §2 step 1): the client certificate's URI SAN must be an allowed SPIFFE id, else
 * {@code PERMISSION_DENIED}. The id is put into {@link #SPIFFE_ID} for the call. Plaintext (local/dev) passes as
 * {@code plaintext}.
 */
public final class SpiffeInterceptor implements ServerInterceptor {

  public static final Context.Key<String> SPIFFE_ID = Context.key("pushapi.spiffe");
  private static final int SAN_URI = 6;

  private final Set<String> allowed;
  private final boolean plaintext;

  public SpiffeInterceptor(List<String> allowed, boolean plaintext) {
    this.allowed = Set.copyOf(allowed);
    this.plaintext = plaintext;
  }

  @Override
  public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
      ServerCallHandler<Q, R> next) {
    Optional<String> id = plaintext ? Optional.of("plaintext")
        : peer(call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION)).filter(allowed::contains);
    if (id.isEmpty()) {
      call.close(Status.PERMISSION_DENIED.withDescription("client SPIFFE id not allowed"), new Metadata());
      return new ServerCall.Listener<>() { };
    }
    return Contexts.interceptCall(Context.current().withValue(SPIFFE_ID, id.get()), call, headers, next);
  }

  static Optional<String> peer(SSLSession ssl) {
    if (ssl == null) {
      return Optional.empty();
    }
    try {
      Certificate[] chain = ssl.getPeerCertificates();
      if (chain == null || chain.length == 0 || !(chain[0] instanceof X509Certificate c)) {
        return Optional.empty();
      }
      Collection<List<?>> sans = c.getSubjectAlternativeNames();
      if (sans == null) {
        return Optional.empty();
      }
      for (List<?> san : sans) {
        if (san.size() == 2 && Integer.valueOf(SAN_URI).equals(san.get(0)) && san.get(1) instanceof String s
            && s.startsWith("spiffe://")) {
          return Optional.of(s);
        }
      }
      return Optional.empty();
    } catch (SSLPeerUnverifiedException | CertificateParsingException e) {
      return Optional.empty();
    }
  }
}
