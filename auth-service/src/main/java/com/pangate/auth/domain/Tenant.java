package com.pangate.auth.domain;

/** A customer tenant, or the operator tenant {@value #OPERATOR}. */
public record Tenant(String id, boolean active) {

  public static final String OPERATOR = "pangate";
  /** Reserved by the gateway's warm-up runner; never a real tenant. */
  public static final String WARMUP = "__warmup";

  public static boolean isOperator(String id) {
    return OPERATOR.equals(id);
  }
}
