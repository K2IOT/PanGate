package com.pangate.account.application.port;

import com.pangate.account.domain.Session;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Device sessions (account spec §2, §3). */
public interface SessionStore {

  void insert(Session s, byte[] refreshHash);

  Optional<Session> byRefreshHash(byte[] hash);

  Optional<Session> byDeviceId(String deviceId);

  /** Sessions not revoked, oldest first. */
  List<Session> live(String userId);

  /** Revokes every live session of the user except {@code exceptDeviceId} (may be null); returns the revoked ids. */
  List<String> revokeAll(String userId, String exceptDeviceId);

  /** False when it was not a live session of that user. */
  boolean revoke(String userId, String deviceId);

  void touch(String deviceId, Instant at);

  /** The device's session is live and belongs to {@code userId}. */
  boolean isLive(String userId, String deviceId);

  /** Revokes sessions last used before {@code before}; returns how many. */
  int expireIdle(Instant before);

  /** Deletes sessions revoked before {@code before}. */
  int purgeRevoked(Instant before);

  long countLive();
}
