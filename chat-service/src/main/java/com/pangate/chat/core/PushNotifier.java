package com.pangate.chat.core;

import com.pangate.contracts.v1.ChatRejected;
import com.pangate.contracts.v1.PushResponse;
import java.util.function.Consumer;

/** {@link Notifier} through the Push API: best effort, the sender's client times the send out otherwise. */
public final class PushNotifier implements Notifier {

  private final Pusher pusher;
  private final Consumer<String> onResult;

  /** {@code onResult}: ok, the failing code, {@code dropped} (no room in the client, perf 3b) or {@code unavailable}. */
  public PushNotifier(Pusher pusher, Consumer<String> onResult) {
    this.pusher = pusher;
    this.onResult = onResult;
  }

  @Override
  public void rejected(String senderId, ChatRejected r) {
    pusher.send(FanOutPlanner.rejected(senderId, r), Pusher.Priority.BEST_EFFORT).whenComplete((res, e) -> onResult.accept(
        e != null ? (cause(e) instanceof PushDroppedException ? "dropped" : "unavailable")
            : res.getCode() == PushResponse.Code.CODE_OK ? "ok" : res.getCode().name()));
  }

  private static Throwable cause(Throwable e) {
    return e instanceof java.util.concurrent.CompletionException && e.getCause() != null ? e.getCause() : e;
  }
}
