package com.pangate.auth.domain;

import java.util.List;

/** A validated connect-token request; build it with {@link ClaimLimits#validate}. */
public record ConnectTokenRequest(String tenant, String principal, String device, List<String> topics, int ttlSec) {}
