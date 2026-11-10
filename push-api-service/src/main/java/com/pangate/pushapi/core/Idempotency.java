package com.pangate.pushapi.core;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.contracts.v1.PushResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * {@code idem:{t}:<key>} (push contracts §3.3, push-api spec §2.1). The first request owns the key with a
 * {@code pending} value that already carries its msg_id and seq; {@code :last} keeps those values for an hour, so a
 * retry after a failed produce or a crash produces the same msg_id and seq again (downstream dedups on msg_id).
 * A finished key holds the first {@link PushResponse}; later requests get it back with {@code duplicate=true}.
 */
public final class Idempotency {

  static final String DONE = "done|";
  static final Duration POLL = Duration.ofMillis(50);

  public sealed interface Claim permits Done, Owned, Busy {}

  public record Done(PushResponse first) implements Claim {}

  public record Owned(String key, String value, Reserved reserved) implements Claim {}

  public record Busy() implements Claim {}

  private final IdemStore store;
  private final Duration ttl;
  private final Duration pendingTtl;
  private final Duration waitFor;
  private final Sleeper sleeper;
  private final LongSupplier monoMs;

  public Idempotency(IdemStore store, Duration ttl, Duration pendingTtl, Duration waitFor, Sleeper sleeper,
      LongSupplier monoMs) {
    this.store = store;
    this.ttl = ttl;
    this.pendingTtl = pendingTtl;
    this.waitFor = waitFor;
    this.sleeper = sleeper;
    this.monoMs = monoMs;
  }

  /** {@code reserve} runs only when no earlier reservation exists for the key. */
  public Claim claim(String tenant, String key, Supplier<Reserved> reserve) {
    String k = PushApiKeys.idem(tenant, key);
    String last = PushApiKeys.idemLast(tenant, key);
    long deadline = monoMs.getAsLong() + waitFor.toMillis();
    while (true) {
      Optional<String> v = store.get(k);
      if (v.isPresent() && v.get().startsWith(DONE)) {
        return new Done(decodeDone(v.get()).toBuilder().setDuplicate(true).build());
      }
      if (v.isPresent()) {
        if (monoMs.getAsLong() >= deadline) {
          return new Busy();
        }
        pause();
        continue;
      }
      Reserved r = store.get(last).filter(s -> s.startsWith(Reserved.PENDING)).map(Reserved::decode)
          .orElseGet(reserve);
      String value = r.encode();
      if (store.setIfAbsent(k, value, pendingTtl)) {
        store.set(last, value, ttl);
        return new Owned(k, value, r);
      }
      // lost the race to another request with the same key: read again
    }
  }

  public void complete(Owned o, PushResponse r) {
    store.set(o.key(), DONE + Base64.getEncoder().encodeToString(r.toByteArray()), ttl);
  }

  public void abandon(Owned o) {
    store.deleteIfEquals(o.key(), o.value());
  }

  private void pause() {
    try {
      sleeper.sleep(POLL);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StoreUnavailableException("interrupted waiting for an idempotency key", e);
    }
  }

  private static PushResponse decodeDone(String v) {
    try {
      return PushResponse.parseFrom(Base64.getDecoder().decode(v.substring(DONE.length())));
    } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
      throw new StoreUnavailableException("corrupt idempotency value", e);
    }
  }
}
