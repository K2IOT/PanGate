package com.pangate.account.api;

/** HTTP status of each stable problem code (account spec §4.2, §5). */
final class Statuses {

  private Statuses() {}

  static int of(String code) {
    return switch (code) {
      case "username_taken", "email_taken", "phone_taken" -> 409;
      case "invalid_credentials", "invalid_session", "invalid_token" -> 401;
      case "account_locked", "password_change_required", "forbidden" -> 403;
      case "not_found", "user_not_found" -> 404;
      case "too_many_attempts", "rate_limited" -> 429;
      case "unavailable" -> 503;
      default -> 400;   // invalid_*, weak_password, invalid_request
    };
  }
}
