package com.pangate.gateway.loop;

/** A wheel entry that reacts when its deadline passes; called on the owning event loop. */
public interface Deadlined {

  void onDeadline(long nowSec);
}
