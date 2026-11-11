package com.pangate.router.delivery;

import com.pangate.contracts.v1.AckItem;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import com.pangate.router.registry.LiveConnection;

/** Writes the record's outcome; returns only once it is durable (push-router spec §0 invariant 1). */
public interface OutcomeRecorder {

  void offline(PushRecord r, OfflineRecord.Reason reason, int attempts);

  void delivered(PushRecord r, LiveConnection c, AckItem.Status status);
}
