package com.pangate.account.api;

import com.pangate.account.domain.DomainException;
import java.util.Map;

/** Typed reads of a JSON object body; a wrong type is {@code invalid_request}. */
final class Bodies {

  private Bodies() {}

  /** The string, or null when absent or JSON null. */
  static String string(Map<String, Object> body, String field) {
    Object v = body.get(field);
    if (v == null) {
      return null;
    }
    if (!(v instanceof String s)) {
      throw new DomainException("invalid_request", field + " must be a string");
    }
    return s;
  }

  static String required(Map<String, Object> body, String field) {
    String s = string(body, field);
    if (s == null) {
      throw new DomainException("invalid_request", field + " is required");
    }
    return s;
  }

  static char[] chars(String s) {
    return s == null ? null : s.toCharArray();
  }
}
