package com.pangate.notify.application.port;

import com.pangate.contracts.v1.NotifyRequest;
import com.pangate.notify.application.SendResult;
import com.pangate.notify.domain.PushToken;
import java.util.concurrent.CompletableFuture;

/** APNs or FCM for one tenant. Never completes exceptionally: a failure is a {@link SendResult}. */
public interface PushProvider {
  CompletableFuture<SendResult> send(PushToken t, NotifyRequest r);
}
