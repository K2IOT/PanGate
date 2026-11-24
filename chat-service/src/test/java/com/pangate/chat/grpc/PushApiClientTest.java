package com.pangate.chat.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.pangate.chat.config.ChatProperties;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.Pusher.Priority;
import com.pangate.contracts.v1.PushRequest;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** Perf 3b (spec §4.2): a pool of streams over round-robin, each push on the stream of its target. */
class PushApiClientTest {

  static ChatProperties.PushApi cfg(String target) {
    return new ChatProperties.PushApi(target, true, Duration.ofSeconds(8), 4, 1_000, 0.2, Duration.ofSeconds(2),
        10_000, Duration.ofSeconds(10));
  }

  static PushRequest req(String principal, String seq) {
    return PushRequest.newBuilder().setRequestId(Ids.next()).setKind(PushRequest.Kind.KIND_MESSAGE)
        .setPrincipalId(principal).putHeaders("seq", seq).build();
  }

  static Server inProcess(String name, ScriptedPushService api) throws Exception {
    return InProcessServerBuilder.forName(name).addService(api).build().start();
  }

  @Test
  void sameTargetAlwaysUsesTheSameStream() throws Exception {
    ScriptedPushService api = new ScriptedPushService();
    api.autoAnswer = true;
    String name = "pool-" + Ids.next();
    Server server = inProcess(name, api);
    try (PushApiClient c = new PushApiClient(InProcessChannelBuilder.forName(name).build(), cfg("x"),
        new SimpleMeterRegistry())) {
      for (int i = 0; i < 1_000; i++) {
        c.send(req("p" + (i % 100), Integer.toString(i)), Priority.MUST);
      }
      await().until(() -> api.received.size() == 1_000);
      Map<String, Set<Integer>> streamsOf = api.received.stream().collect(Collectors.groupingBy(
          r -> r.request().getPrincipalId(), Collectors.mapping(ScriptedPushService.Received::stream,
              Collectors.toSet())));
      assertThat(streamsOf.values()).allSatisfy(s -> assertThat(s).hasSize(1));
      assertThat(api.received.stream().map(ScriptedPushService.Received::stream).distinct().count()).isEqualTo(4);
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  void orderPerRecipientIsKept() throws Exception {
    ScriptedPushService api = new ScriptedPushService();
    api.autoAnswer = true;
    String name = "pool-" + Ids.next();
    Server server = inProcess(name, api);
    try (PushApiClient c = new PushApiClient(InProcessChannelBuilder.forName(name).build(), cfg("x"),
        new SimpleMeterRegistry())) {
      List<Thread> ts = new ArrayList<>();
      for (int t = 0; t < 4; t++) {
        int thread = t;
        ts.add(Thread.ofPlatform().start(() -> {
          for (int i = 0; i < 200; i++) {
            c.send(req("p", thread + ":" + i), Priority.MUST);
          }
        }));
      }
      for (Thread t : ts) {
        t.join();
      }
      await().until(() -> api.received.size() == 800);
      for (int t = 0; t < 4; t++) {
        String prefix = t + ":";
        List<Integer> order = api.requests().stream().map(r -> r.getHeadersMap().get("seq"))
            .filter(s -> s.startsWith(prefix)).map(s -> Integer.parseInt(s.substring(prefix.length()))).toList();
        assertThat(order).hasSize(200).isSorted();
      }
    } finally {
      server.shutdownNow();
    }
  }

  @Test
  void broadcastKeyIsTheTopic() {
    PushRequest b = PushRequest.newBuilder().setKind(PushRequest.Kind.KIND_BROADCAST).setTopic("chan:c1").build();
    assertThat(PushApiClient.streamKey(b)).isEqualTo("topic:chan:c1");
    assertThat(PushApiClient.streamKey(req("u1", "0"))).isEqualTo("u1");
  }

  /** Two Push API addresses behind one name; round-robin puts streams on both (after a reconnect at the latest). */
  @Test
  void streamsSpreadOverServers() throws Exception {
    ScriptedPushService a = new ScriptedPushService();
    ScriptedPushService b = new ScriptedPushService();
    a.autoAnswer = true;
    b.autoAnswer = true;
    Server sa = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0)).addService(a).build().start();
    Server sb = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0)).addService(b).build().start();
    String scheme = "pushtest" + Math.abs(Ids.next().hashCode());
    NameResolverRegistry.getDefaultRegistry().register(new StaticResolver(scheme, List.of(
        new InetSocketAddress("127.0.0.1", sa.getPort()), new InetSocketAddress("127.0.0.1", sb.getPort()))));
    try (PushApiClient c = PushApiClient.create(cfg(scheme + ":///push-api"), new SimpleMeterRegistry())) {
      for (int i = 0; i < 400; i++) {
        c.send(req("p" + i, "0"), Priority.MUST);
      }
      await().until(() -> a.received.size() + b.received.size() == 400);
      if (a.streams.get() == 0 || b.streams.get() == 0) {
        (a.streams.get() == 0 ? b : a).endAll();   // as a connection age would: the streams reopen and spread
      }
      int[] n = {0};
      await().until(() -> {
        for (int i = 0; i < 50; i++) {
          c.send(req("p" + (n[0]++ % 400), "1"), Priority.MUST);
        }
        return a.streams.get() > 0 && b.streams.get() > 0;
      });
    } finally {
      sa.shutdownNow();
      sb.shutdownNow();
    }
  }

  static final class StaticResolver extends NameResolverProvider {
    final String scheme;
    final List<SocketAddress> addresses;

    StaticResolver(String scheme, List<SocketAddress> addresses) {
      this.scheme = scheme;
      this.addresses = addresses;
    }

    @Override
    protected boolean isAvailable() {
      return true;
    }

    @Override
    protected int priority() {
      return 5;
    }

    @Override
    public String getDefaultScheme() {
      return scheme;
    }

    @Override
    public NameResolver newNameResolver(URI uri, NameResolver.Args args) {
      if (!scheme.equals(uri.getScheme())) {
        return null;
      }
      return new NameResolver() {
        @Override
        public String getServiceAuthority() {
          return "push-api";
        }

        @Override
        public void start(Listener2 listener) {
          listener.onResult(ResolutionResult.newBuilder().setAddresses(addresses.stream()
              .map(EquivalentAddressGroup::new).toList()).build());
        }

        @Override
        public void shutdown() {}
      };
    }
  }
}
