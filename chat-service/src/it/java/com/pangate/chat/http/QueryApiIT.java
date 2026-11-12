package com.pangate.chat.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.FanOutPlanner;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.MemberLookup;
import com.pangate.chat.kafka.SequencerLoop;
import com.pangate.chat.store.RedisChatCache;
import com.pangate.chat.store.ScyllaChatStore;
import com.pangate.chat.support.ChatStack;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PushRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class QueryApiIT {

  static ChatStack s;

  @BeforeAll
  static void boot() throws Exception {
    s = new ChatStack().start();
  }

  @AfterAll
  static void shutdown() {
    s.close();
  }

  static String group(String owner, String... members) {
    HttpResponse<String> r = s.call("POST", "/v1/conversations", owner, Map.of("kind", "group", "title", "g",
        "memberIds", List.of(members)));
    assertThat(r.statusCode()).as(r.body()).isEqualTo(201);
    return s.json(r).get("id").asText();
  }

  static List<Long> seqs(JsonNode messages) {
    List<Long> out = new ArrayList<>();
    messages.forEach(m -> out.add(m.get("convSeq").asLong()));
    return out;
  }

  @Test
  void historyPagesByConvSeq() {
    String g = group("ha", "hb");
    for (int i = 1; i <= 12; i++) {
      s.send(i % 2 == 0 ? "ha" : "hb", g, "m" + i);
    }
    JsonNode latest = s.json(s.call("GET", "/v1/conversations/" + g + "/messages?limit=5", "hb", null));
    assertThat(seqs(latest.get("messages"))).containsExactly(12L, 11L, 10L, 9L, 8L);
    assertThat(latest.get("lastSeq").asLong()).isEqualTo(12);
    JsonNode older = s.json(s.call("GET", "/v1/conversations/" + g + "/messages?before=8&limit=5", "hb", null));
    assertThat(seqs(older.get("messages"))).containsExactly(7L, 6L, 5L, 4L, 3L);
    JsonNode newer = s.json(s.call("GET", "/v1/conversations/" + g + "/messages?after=9", "hb", null));
    assertThat(seqs(newer.get("messages"))).containsExactly(10L, 11L, 12L);
    assertThat(newer.get("messages").get(0).get("text").asText()).isEqualTo("m10");
    JsonNode sync = s.json(s.call("POST", "/v1/sync", "ha", Map.of("conversations", List.of(
        Map.of("id", g, "lastSeq", 10), Map.of("id", g + "X", "lastSeq", 0)))));
    assertThat(sync.get("conversations")).hasSize(1);
    assertThat(seqs(sync.get("conversations").get(0).get("messages"))).containsExactly(11L, 12L);
    assertThat(s.call("GET", "/v1/conversations/" + g + "/messages?before=3&after=1", "ha", null).statusCode())
        .isEqualTo(400);
    assertThat(s.call("GET", "/v1/conversations/" + g + "/messages?limit=101", "ha", null).statusCode())
        .isEqualTo(400);
  }

  @Test
  void conversationsSortedByLastMessage() {
    String a = group("su", "sx");
    String b = group("su", "sy");
    String dm = s.json(s.call("POST", "/v1/dm/sz", "su", null)).get("id").asText();
    assertThat(s.json(s.call("POST", "/v1/dm/su", "sz", null)).get("id").asText()).isEqualTo(dm);
    s.send("su", a, "1");
    s.send("su", dm, "2");
    s.send("su", b, "3");
    // user_convs rows move after fan-out reached the Push API
    await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
      JsonNode list = s.json(s.call("GET", "/v1/conversations", "su", null)).get("conversations");
      List<String> ids = new ArrayList<>();
      list.forEach(c -> ids.add(c.get("id").asText()));
      assertThat(ids).containsExactly(b, dm, a);
      assertThat(list.get(0).get("lastSeq").asLong()).isEqualTo(1);
    });
    JsonNode page = s.json(s.call("GET", "/v1/conversations?limit=2", "su", null));
    assertThat(page.get("conversations")).hasSize(2);
    JsonNode rest = s.json(s.call("GET", "/v1/conversations?limit=2&cursor=" + page.get("nextCursor").asText(),
        "su", null));
    assertThat(rest.get("conversations")).hasSize(1);
    assertThat(rest.get("conversations").get(0).get("id").asText()).isEqualTo(a);
    JsonNode info = s.json(s.call("GET", "/v1/conversations/" + dm, "sz", null));
    assertThat(info.get("kind").asText()).isEqualTo("direct");
    assertThat(info.get("memberCount").asInt()).isEqualTo(2);
  }

  /** Review Focus 2: removal evicts the cached member list at once; the next message is not for B. */
  @Test
  void removedMemberStopsReceivingAtOnce() {
    String g = group("ra", "rb", "rc");
    s.send("rb", g, "warms the member cache");
    RedisChatCache cache = s.chat.getBean(RedisChatCache.class);
    assertThat(cache.cachedMembers(g)).get().asList().hasSize(3);
    assertThat(s.call("DELETE", "/v1/conversations/" + g + "/members/rb", "ra", null).statusCode()).isEqualTo(204);
    MemberLookup lookup = new MemberLookup(s.chat.getBean(ScyllaChatStore.class), cache);
    StoredMessage next = new StoredMessage(g, 2, Ids.next(), "ra", Ids.next(), "after", "", List.of(), 1);
    List<PushRequest> plan = FanOutPlanner.plan(SequencerLoop.event(next, ConversationKind.CONVERSATION_KIND_GROUP),
        lookup.of(g));
    assertThat(plan).extracting(PushRequest::getPrincipalId).containsExactlyInAnyOrder("chat:ra", "chat:rc");
    HttpResponse<String> late = s.call("POST", "/v1/conversations/" + g + "/messages", s.token("rb", "d1"),
        Map.of("text", "still here?"), Map.of("Idempotency-Key", Ids.next()));
    assertThat(late.statusCode()).isEqualTo(403);
    assertThat(s.call("GET", "/v1/conversations/" + g + "/messages", "rb", null).statusCode()).isEqualTo(403);
    // rb may still leave a conversation it is no longer in: no error, nothing changes
    assertThat(s.call("DELETE", "/v1/conversations/" + g + "/members/rc", "rc", null).statusCode()).isEqualTo(204);
  }

  @Test
  void problemJsonCodes() {
    String g = group("pa", "pb");
    HttpResponse<String> noToken = s.call("GET", "/v1/conversations", (String) null, null, Map.of());
    assertThat(noToken.statusCode()).isEqualTo(401);
    assertThat(noToken.headers().firstValue("Content-Type")).get().asString().startsWith("application/problem+json");
    assertThat(s.json(noToken).get("code").asText()).isEqualTo("invalid_token");
    HttpResponse<String> notMine = s.call("GET", "/v1/conversations/" + g, "stranger", null);
    assertThat(notMine.statusCode()).isEqualTo(403);
    assertThat(s.json(notMine).get("code").asText()).isEqualTo("forbidden");
    HttpResponse<String> none = s.call("GET", "/v1/conversations/" + Ids.next(), "pa", null);
    assertThat(none.statusCode()).isEqualTo(404);
    assertThat(s.json(none).get("code").asText()).isEqualTo("not_found");
    HttpResponse<String> bad = s.call("POST", "/v1/conversations", "pa", Map.of("kind", "dm"));
    assertThat(bad.statusCode()).isEqualTo(400);
    assertThat(s.json(bad).get("code").asText()).isEqualTo("invalid_request");
    assertThat(s.call("POST", "/v1/conversations/" + g + "/members", "pb", Map.of("userId", "x")).statusCode())
        .isEqualTo(403);
    String ch = s.json(s.call("POST", "/v1/conversations", "pa", Map.of("kind", "channel", "title", "news",
        "public", true))).get("id").asText();
    assertThat(s.call("POST", "/v1/conversations/" + ch + "/join", "fan", null).statusCode()).isEqualTo(204);
    assertThat(s.call("GET", "/v1/conversations/" + ch, "anyone", null).statusCode()).isEqualTo(200);
    assertThat(s.call("POST", "/v1/conversations/" + g + "/join", "fan", null).statusCode()).isEqualTo(403);
  }
}
