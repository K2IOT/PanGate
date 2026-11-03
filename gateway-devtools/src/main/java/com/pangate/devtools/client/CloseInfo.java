package com.pangate.devtools.client;

/** How the gateway closed a connection: WS close code, or ErrorInfo.code of a fatal TCP ERROR frame. 1006 = no close info. */
public record CloseInfo(int code, String reason) {

  public static final CloseInfo ABNORMAL = new CloseInfo(1006, "");
}
