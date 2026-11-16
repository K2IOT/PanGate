package com.pangate.account.domain;

import java.time.Instant;

/**
 * An account row with its identifiers (account spec §3). {@code username} is the spelling chosen by the user;
 * personal fields are null once deleted. {@code email} / {@code phone} may be null.
 */
public record Account(String userId, String username, String displayName, String bio, String passwordHash,
    boolean mustChangePassword, AccountStatus status, String email, String phone, Instant createdAt,
    Instant deletedAt) {}
