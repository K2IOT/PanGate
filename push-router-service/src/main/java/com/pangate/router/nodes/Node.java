package com.pangate.router.nodes;

/** A live gateway node: lease epoch, gRPC address {@code host:port}, draining flag. */
public record Node(String id, long epoch, String address, boolean draining) {}
