package com.pangate.devtools.client;

import com.pangate.contracts.v1.Hello;

/** HELLO payload builders for dev clients. */
public final class Hellos {

  public static final String SDK_VERSION = "devtools-0.1.0";

  private Hellos() {}

  public static Hello jwt(String jwt, String deviceId) {
    return base(deviceId).setJwt(jwt).build();
  }

  public static Hello ticket(String ticket, String deviceId) {
    return base(deviceId).setResumeTicket(ticket).build();
  }

  private static Hello.Builder base(String deviceId) {
    return Hello.newBuilder().setDeviceId(deviceId).setSdkVersion(SDK_VERSION).setPlatform("iot");
  }
}
