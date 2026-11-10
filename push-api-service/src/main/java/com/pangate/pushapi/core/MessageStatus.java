package com.pangate.pushapi.core;

import com.pangate.contracts.v1.DeliveredRecord;
import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.contracts.v1.PushRecord;
import java.util.Map;
import java.util.Optional;

/** {@code msg:{t}:<id>} (push-api spec §4.2); every call may throw {@link StoreUnavailableException}. */
public interface MessageStatus {

  void accepted(PushRecord r);

  Optional<Map<String, String>> get(String tenant, String msgId);

  void offline(OfflineRecord o);

  void delivered(DeliveredRecord d);
}
