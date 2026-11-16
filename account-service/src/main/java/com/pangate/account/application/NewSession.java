package com.pangate.account.application;

/** A device that just logged in or registered (account spec §4.2): the refresh token is shown once. */
public record NewSession(String userId, String deviceId, String refreshToken) {}
