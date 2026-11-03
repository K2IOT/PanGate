package com.pangate.gateway.session;

/** Where a session lives: its event loop index and session key. */
public record ConnRef(int loopIdx, long sessionKey) {}
