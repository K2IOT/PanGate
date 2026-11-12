package com.pangate.chat.core;

import com.pangate.contracts.v1.ChatRejected;
import com.pangate.contracts.v1.PushResponse;
import java.util.function.Consumer;

/** {@link Notifier} through the Push API: best effort, the sender's client times the send out otherwise. */
public final class PushNotifier implements Notifier {

  private final Pusher pusher;
  private final Consumer<String> onResult;

  /** {@code onResult}: ok, or the failing code / {@code unavailable} (metric). */
  public PushNotifier(Pusher pusher, Consumer<String> onResult) {
    this.pusher = pusher;
    this.onResult = onResult;
  }

  @Override
  public void rejected(String senderId, ChatRejected r) {
    pusher.send(FanOutPlanner.rejected(senderId, r)).whenComplete((res, e) -> onResult.accept(e != null
        ? "unavailable" : res.getCode() == PushResponse.Code.CODE_OK ? "ok" : res.getCode().name()));
  }
}
