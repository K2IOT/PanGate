package com.pangate.devtools.cmd;

import io.grpc.ManagedChannel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "health",
    description = "Exit 0 if the URL answers HTTP 200, or the gRPC server reports SERVING, within 2 s; else 1.")
public final class HealthCommand implements Callable<Integer> {

  @ArgGroup(exclusive = true, multiplicity = "1")
  Target target;

  static final class Target {
    @Parameters(index = "0", description = "HTTP URL")
    URI url;

    @Option(names = "--grpc", description = "host:port of a gRPC server with the standard health service")
    String grpc;
  }

  @Override
  public Integer call() {
    return target.grpc != null ? grpc(target.grpc) : http(target.url);
  }

  private static int http(URI url) {
    try {
      HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
      HttpResponse<Void> res = client.send(
          HttpRequest.newBuilder(url).timeout(Duration.ofSeconds(2)).build(), HttpResponse.BodyHandlers.discarding());
      return res.statusCode() == 200 ? 0 : 1;
    } catch (Exception e) {
      return 1;
    }
  }

  private static int grpc(String address) {
    ManagedChannel ch = NettyChannelBuilder.forTarget(address).usePlaintext().build();
    try {
      HealthCheckResponse r = HealthGrpc.newBlockingStub(ch).withDeadlineAfter(2, TimeUnit.SECONDS)
          .check(HealthCheckRequest.getDefaultInstance());
      return r.getStatus() == HealthCheckResponse.ServingStatus.SERVING ? 0 : 1;
    } catch (RuntimeException e) {
      return 1;
    } finally {
      ch.shutdownNow();
    }
  }
}
