package com.pangate.notify.application;

/** The device behind a connect token: tenant, principal and device come from the token, never from the body. */
public record Caller(String tenantId, String principalId, String deviceId, String jti) {}
