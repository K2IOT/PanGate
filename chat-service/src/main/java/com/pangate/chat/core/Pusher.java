package com.pangate.chat.core;

import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import java.util.concurrent.CompletableFuture;

/**
 * Sends to the Push API. Asynchronous: a group message is up to 500 requests, sent together and awaited together.
 * The future completes with the Push API's answer (any code), or exceptionally with
 * {@link PushUnavailableException} when no answer came (stream down, timeout).
 */
@FunctionalInterface
public interface Pusher {
  CompletableFuture<PushResponse> send(PushRequest r);
}
