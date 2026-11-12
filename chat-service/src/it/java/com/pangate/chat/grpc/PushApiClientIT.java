package com.pangate.chat.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.FanOutPlanner;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.contracts.v1.ChatEvent;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.PushResponse;
import com.pangate.pushapi.PushApiApplication;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** {@link PushApiClient} against the real Push API in this JVM (plaintext gRPC). */
class PushApiClientIT {

  static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));
  static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4.2"))
      .withExposedPorts(6379);
  static ConfigurableApplicationContext pushApi;
  static int grpc;

  static int freePort() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  @BeforeAll
  static void boot() throws Exception {
    kafka.start();
    redis.start();
    try (Admin a = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      a.createTopics(List.of(new NewTopic("gw.control", 2, (short) 1), new NewTopic("c1.push.requests", 4, (short) 1)))
          .all().get();
    }
    grpc = freePort();
    pushApi = new SpringApplicationBuilder(PushApiApplication.class).profiles("local").run(
        "--spring.config.name=push-api", "--server.port=" + freePort(), "--push-api.management-port=" + freePort(),
        "--push-api.grpc.port=" + grpc, "--push-api.kafka.bootstrap=" + kafka.getBootstrapServers(),
        "--push-api.redis.uri=redis://" + redis.getHost() + ":" + redis.getMappedPort(6379),
        "--push-api.auth.jwks-uri=http://127.0.0.1:1/none",
        // chat-service's ScyllaDB driver is on this classpath; Push API has no use for it
        "--spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration");
  }

  @AfterAll
  static void shutdown() {
    if (pushApi != null) {
      pushApi.close();
    }
    redis.stop();
    kafka.stop();
  }

  @Test
  void answersMatchedByRequestId() throws Exception {
    try (PushApiClient c = new PushApiClient(NettyChannelBuilder.forAddress("127.0.0.1", grpc).usePlaintext()
        .build(), Duration.ofSeconds(5))) {
      List<Member> members = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        members.add(new Member("u" + i, Role.MEMBER));
      }
      ChatEvent e = ChatEvent.newBuilder().setKind(ConversationKind.CONVERSATION_KIND_GROUP).setMessage(ChatMessage
          .newBuilder().setConversationId(Ids.next()).setMsgId(Ids.next()).setConvSeq(1).setSenderId("u0")
          .setText("hi")).build();
      List<PushRequest> rs = new ArrayList<>(FanOutPlanner.plan(e, members));
      rs.add(FanOutPlanner.plan(e.toBuilder().setKind(ConversationKind.CONVERSATION_KIND_CHANNEL).build(), List.of())
          .getFirst());
      rs.add(rs.getFirst().toBuilder().setRequestId(Ids.next()).setPrincipalId("").build());   // invalid
      List<CompletableFuture<PushResponse>> fs = rs.stream().map(c::send).toList();
      for (int i = 0; i < rs.size(); i++) {
        PushResponse r = fs.get(i).get(10, TimeUnit.SECONDS);
        assertThat(r.getRequestId()).isEqualTo(rs.get(i).getRequestId());
        if (i < rs.size() - 1) {
          assertThat(r.getCode()).as(r.getError()).isEqualTo(PushResponse.Code.CODE_OK);
          assertThat(r.getMsgId()).isNotEmpty();
        } else {
          assertThat(r.getCode()).isEqualTo(PushResponse.Code.CODE_INVALID);
        }
      }
    }
  }
}
