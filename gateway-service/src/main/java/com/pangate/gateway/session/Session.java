package com.pangate.gateway.session;

import com.pangate.gateway.loop.Deadlined;
import com.pangate.gateway.loop.WheelNode;

/** Placeholder until Task 14 provides the full session; only what LoopContext needs. */
public class Session extends WheelNode implements Deadlined {

  private final long sessionKey;

  public Session(long sessionKey) {
    this.sessionKey = sessionKey;
  }

  public long sessionKey() {
    return sessionKey;
  }

  @Override
  public void onDeadline(long nowSec) {}
}
