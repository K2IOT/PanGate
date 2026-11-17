package com.pangate.account.application;

import com.pangate.account.application.port.SessionStore;
import com.pangate.account.domain.DeviceInfo;
import com.pangate.account.domain.Ids;
import com.pangate.account.domain.Session;
import com.pangate.account.infrastructure.crypto.RefreshTokens;
import java.time.Instant;

/** Opens a device session: a new ULID deviceId and a refresh token of which only the hash is stored. */
final class Sessions {

  private Sessions() {}

  static NewSession open(SessionStore store, String userId, DeviceInfo d, Instant now) {
    String deviceId = Ids.next();
    String token = RefreshTokens.newToken();
    store.insert(new Session(deviceId, userId, d.deviceName(), d.platform(), now, now, null),
        RefreshTokens.hash(token));
    return new NewSession(userId, deviceId, token);
  }
}
