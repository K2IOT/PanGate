package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.v1.ChatRejected;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/** Perf 3b (spec §4.1): each caller says how much its push matters. */
class PusherPrioritiesTest {

  final List<Pusher.Priority> seen = new CopyOnWriteArrayList<>();
  final Pusher recording = (r, p) -> {
    seen.add(p);
    return CompletableFuture.completedFuture(PushResponse.getDefaultInstance());
  };

  @Test
  void defaultSendIsMust() {
    recording.send(PushRequest.getDefaultInstance());
    assertThat(seen).containsExactly(Pusher.Priority.MUST);
  }

  @Test
  void notifierIsBestEffort() {
    new PushNotifier(recording, x -> { }).rejected("u1", ChatRejected.newBuilder().setReason("too_long").build());
    assertThat(seen).containsExactly(Pusher.Priority.BEST_EFFORT);
  }

  /** 3b minor: a rejection the client dropped for lack of room is counted as dropped, not unavailable. */
  @Test
  void droppedRejectionIsCountedAsDropped() {
    java.util.List<String> results = new java.util.ArrayList<>();
    new PushNotifier((r, p) -> java.util.concurrent.CompletableFuture.failedFuture(
        new PushDroppedException(PushDroppedException.WAITED)), results::add)
        .rejected("u1", ChatRejected.newBuilder().setReason("too_long").build());
    assertThat(results).containsExactly("dropped");
  }
}
