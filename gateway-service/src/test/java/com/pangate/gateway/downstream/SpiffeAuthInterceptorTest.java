package com.pangate.gateway.downstream;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.gateway.test.TestPki;
import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.Status;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SpiffeAuthInterceptorTest {

  @TempDir Path dir;

  private final AtomicReference<Status> closed = new AtomicReference<>();
  private boolean handled;

  private ServerCall<Object, Object> call(SSLSession ssl) {
    Attributes attrs = ssl == null ? Attributes.EMPTY
        : Attributes.newBuilder().set(Grpc.TRANSPORT_ATTR_SSL_SESSION, ssl).build();
    return new ServerCall<>() {
      @Override public void request(int n) { }
      @Override public void sendHeaders(Metadata h) { }
      @Override public void sendMessage(Object m) { }
      @Override public void close(Status s, Metadata t) { closed.set(s); }
      @Override public boolean isCancelled() { return false; }
      @Override public Attributes getAttributes() { return attrs; }
      @Override public MethodDescriptor<Object, Object> getMethodDescriptor() { return null; }
    };
  }

  private static SSLSession session(Certificate peer) {
    return (SSLSession) java.lang.reflect.Proxy.newProxyInstance(SSLSession.class.getClassLoader(),
        new Class<?>[] {SSLSession.class}, (p, m, a) -> m.getName().equals("getPeerCertificates")
            ? new Certificate[] {peer} : null);
  }

  private void intercept(SSLSession ssl) {
    new SpiffeAuthInterceptor(List.of("spiffe://pangate/cell-1/push-router"))
        .interceptCall(call(ssl), new Metadata(), (c, h) -> {
          handled = true;
          return new ServerCall.Listener<>() { };
        });
  }

  @Test
  void allowedSpiffeIdPasses() throws Exception {
    TestPki pki = new TestPki(dir);
    intercept(session(pki.client("router", "spiffe://pangate/cell-1/push-router").cert()));
    assertThat(handled).isTrue();
    assertThat(closed.get()).isNull();
  }

  @Test
  void otherSpiffeIdOrNoSslSessionIsUnauthenticated() throws Exception {
    TestPki pki = new TestPki(dir);
    intercept(session(pki.client("evil", "spiffe://pangate/cell-1/other").cert()));
    assertThat(handled).isFalse();
    assertThat(closed.get().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
    closed.set(null);
    intercept(null);
    assertThat(handled).isFalse();
    assertThat(closed.get().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
  }
}
