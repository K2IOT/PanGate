package com.pangate.account.application.port;

import java.time.Instant;
import java.util.List;

/** admin_actions (account spec §5). */
public interface AdminLog {

  record Entry(String actor, String action, String reason, Instant at) {}

  void record(String actor, String action, String userId, String reason);

  List<Entry> of(String userId);
}
