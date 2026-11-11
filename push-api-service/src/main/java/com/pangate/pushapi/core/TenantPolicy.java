package com.pangate.pushapi.core;

/** Effective push policy of one tenant (push-api spec §4.1). Rates are records/s per Push API pod. */
public record TenantPolicy(boolean blocked, boolean broadcastEnabled, int pushRate, int pushBurst, int broadcastRate) {}
