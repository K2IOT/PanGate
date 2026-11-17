package com.pangate.notify.domain;

import java.util.regex.Pattern;

/** Offline notifications spec §5.1: APNs tokens are 64-200 hex characters, FCM tokens at most 4 096 characters. */
public final class TokenRules {

  public static final String PRODUCTION = "production";
  public static final String SANDBOX = "sandbox";
  private static final Pattern APNS = Pattern.compile("[0-9a-fA-F]{64,200}");
  static final int FCM_MAX = 4_096;

  private TokenRules() {}

  public static void validate(Platform p, String token, String environment) {
    if (token == null || token.isEmpty()) {
      throw new DomainException("invalid_request", "token is required");
    }
    switch (p) {
      case APNS -> {
        if (!APNS.matcher(token).matches()) {
          throw new DomainException("invalid_request", "an APNs token is 64-200 hex characters");
        }
        if (!PRODUCTION.equals(environment) && !SANDBOX.equals(environment)) {
          throw new DomainException("invalid_request", "environment must be production or sandbox");
        }
      }
      case FCM -> {
        if (token.length() > FCM_MAX) {
          throw new DomainException("invalid_request", "an FCM token is at most 4096 characters");
        }
        if (!PRODUCTION.equals(environment)) {
          throw new DomainException("invalid_request", "FCM has no sandbox environment");
        }
      }
    }
  }
}
