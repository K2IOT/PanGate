package com.pangate.notify.application.port;

/** True the first time a request id is seen (spec §5.4, invariant 4). May throw when the store is down. */
public interface Dedup {
  boolean firstTime(String requestId);
}
