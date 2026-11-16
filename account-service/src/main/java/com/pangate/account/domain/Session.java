package com.pangate.account.domain;

import java.time.Instant;

/** One logged-in device (account spec §2, §3); {@code deviceId} is the {@code device} of its connect tokens. */
public record Session(String deviceId, String userId, String deviceName, String platform, Instant createdAt,
    Instant lastUsedAt, Instant revokedAt) {}
