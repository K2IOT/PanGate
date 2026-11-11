package com.pangate.pushapi.core;

/** A principal's seq and the epoch it belongs to. */
public record Seq(long seq, int epoch) {}
