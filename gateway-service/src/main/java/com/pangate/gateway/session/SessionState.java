package com.pangate.gateway.session;

/** Session lifecycle (spec §5.1). DRAINING arrives with M3. */
public enum SessionState {
  HANDSHAKING,
  ACTIVE,
  CLOSED
}
