package com.pangate.notify.domain;

import java.util.Locale;

/** Where a push token sends: Apple Push Notification service or Firebase Cloud Messaging. */
public enum Platform {
  APNS, FCM;

  /** The value in the API and the database: {@code apns} / {@code fcm}. */
  public String wire() {
    return name().toLowerCase(Locale.ROOT);
  }

  public static Platform parse(String s) {
    if (s != null) {
      for (Platform p : values()) {
        if (p.wire().equals(s)) {
          return p;
        }
      }
    }
    throw new DomainException("invalid_request", "platform must be apns or fcm");
  }
}
