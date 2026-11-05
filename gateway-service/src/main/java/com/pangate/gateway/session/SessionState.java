package com.pangate.gateway.session;

/** Session lifecycle (spec §5.1). DRAINING: told to reconnect (cell migration; drain and recycling in M3). */
public enum SessionState {
  HANDSHAKING,
  ACTIVE,
  DRAINING,
  CLOSED
}
