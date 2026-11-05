package com.pangate.gateway.diagnostics;

/** A JVM flag the active profile requires is missing (e.g. {@code MaxDirectMemorySize} with profile prod). */
public final class JvmConfigException extends RuntimeException {

  private final String flag;

  public JvmConfigException(String flag) {
    super("-XX:" + flag + " is not set");
    this.flag = flag;
  }

  public String flag() {
    return flag;
  }
}
