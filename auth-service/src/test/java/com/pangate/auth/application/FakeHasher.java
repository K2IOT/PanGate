package com.pangate.auth.application;

import com.pangate.auth.application.port.SecretHasher;
import java.util.concurrent.atomic.AtomicInteger;

final class FakeHasher implements SecretHasher {
  final AtomicInteger burns = new AtomicInteger();

  @Override
  public String hash(char[] secret) {
    return "h:" + new String(secret);
  }

  @Override
  public boolean matches(char[] secret, String hash) {
    return hash != null && hash.equals(hash(secret));
  }

  @Override
  public void burn() {
    burns.incrementAndGet();
  }
}
