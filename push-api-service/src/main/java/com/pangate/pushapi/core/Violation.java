package com.pangate.pushapi.core;

/** A broken rule of push contracts §3.1: {@code rule} is "V1"…"V14". */
public record Violation(String rule, String message) {}
