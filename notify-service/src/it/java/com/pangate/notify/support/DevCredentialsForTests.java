package com.pangate.notify.support;

import com.pangate.notify.config.DevCredentials;
import java.nio.file.Files;
import java.nio.file.Path;

/** An APNs .p8 key and an FCM service account file, generated once per IT JVM. */
public final class DevCredentialsForTests {

  private static Path dir;

  private DevCredentialsForTests() {}

  public static synchronized Path dir() {
    if (dir == null) {
      try {
        dir = Files.createTempDirectory("notify-it-creds");
        DevCredentials.apnsKey(dir.resolve("apns.p8"));
        DevCredentials.fcmServiceAccount(dir.resolve("fcm.json"), "http://unused/token");
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    return dir;
  }
}
