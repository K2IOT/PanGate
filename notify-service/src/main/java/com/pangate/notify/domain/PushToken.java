package com.pangate.notify.domain;

import java.time.Instant;

/** The push token of one device (offline notifications spec §5.2). */
public record PushToken(String tenantId, String principalId, String deviceId, Platform platform, String environment,
    String token, Instant updatedAt) {}
