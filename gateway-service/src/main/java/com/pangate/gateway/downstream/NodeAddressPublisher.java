package com.pangate.gateway.downstream;

import com.pangate.gateway.registry.NodeIdentity;
import org.springframework.context.SmartLifecycle;

/** After the gRPC server bound its port, publishes {@code node:addr:{id}} = advertise-host:port (push-router §3.1). */
public final class NodeAddressPublisher implements SmartLifecycle {

  private final NodeIdentity identity;
  private final GrpcServerLifecycle grpc;
  private final String advertiseHost;
  private volatile boolean running;

  public NodeAddressPublisher(NodeIdentity identity, GrpcServerLifecycle grpc, String advertiseHost) {
    this.identity = identity;
    this.grpc = grpc;
    this.advertiseHost = advertiseHost;
  }

  @Override
  public void start() {
    identity.advertise(advertiseHost + ":" + grpc.port());
    running = true;
  }

  @Override
  public void stop() {
    running = false;  // NodeIdentity.stop deletes the key with the lease
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public int getPhase() {
    return GrpcServerLifecycle.PHASE + 1;
  }
}
