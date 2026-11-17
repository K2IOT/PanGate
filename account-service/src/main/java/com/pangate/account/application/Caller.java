package com.pangate.account.application;

/** Who calls an authenticated endpoint: the user and the device of the connect token (account spec §2). */
public record Caller(String userId, String deviceId, String ip) {}
