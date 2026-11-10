package com.pangate.auth.domain;

import java.time.Duration;

/** Key life cycle durations (spec §3, §8). */
public record KeyPolicy(Duration lifetime, Duration overlap, Duration publishAhead) {}
