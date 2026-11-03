package com.pangate.gateway.pipeline;

/** Test access to the package-private shared ErrorTerminator. */
public final class ErrorTerminatorAccess {

  private ErrorTerminatorAccess() {}

  public static ErrorTerminator create() {
    return new ErrorTerminator();
  }
}
