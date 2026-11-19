package com.pangate.chat.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.pangate.chat.support.ChatStack;
import com.pangate.chat.support.KnownUsers;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** CH3 spec §3.4, §4, §7.2, §8 through the real HTTP API. */
class StateApiIT {

  static ChatStack s;
  static final List<String> crowd = new ArrayList<>();

  @BeforeAll
  static void boot() throws Exception {
    s = new ChatStack().start();
    for (int i = 0; i < 60; i++) {
      crowd.add("cr" + i);
    }
    List<String> all = new ArrayList<>(crowd);
    all.addAll(List.of("la", "lb", "ta", "tb", "tc", "ha", "hb", "va", "vb", "ja", "jb", "jc", "jd", "xa", "xb"));
    KnownUsers.add(s.kafka.getBootstrapServers(), s.chat, all.toArray(String[]::new));
  }

  @AfterAll
  static void shutdown() {
    s.close();
  }

  static String dm(String a, String b) {
    return s.json(s.call("POST", "/v1/dm/" + b, a, null)).get("id").asText();
  }

  static String group(String owner, List<String> members) {
    HttpResponse<String> r = s.call("POST", "/v1/conversations", owner, Map.of("kind", "group", "title", "g",
        "memberIds", members));
    assertThat(r.statusCode()).as(r.body()).isEqualTo(201);
    return s.json(r).get("id").asText();
  }

  static void read(String user, String conv, long seq) {
    assertThat(s.call("POST", "/v1/conversations/" + conv + "/read", user, Map.of("upToSeq", seq)).statusCode())
        .isEqualTo(204);
  }

  static JsonNode row(String user, String conv) {
    for (JsonNode c : s.json(s.call("GET", "/v1/conversations", user, null)).get("conversations")) {
      if (c.get("id").asText().equals(conv)) {
        return c;
      }
    }
    throw new AssertionError(conv + " not listed for " + user);
  }

  @Test
  void listCarriesUnreadAndPeer() {
    String d = dm("la", "lb");
    for (int i = 0; i < 3; i++) {
      s.send("la", d, "m" + i);
    }
    read("lb", d, 2);
    await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
      JsonNode b = row("lb", d);
      assertThat(b.get("readSeq").asLong()).isEqualTo(2);
      assertThat(b.get("unread").asInt()).isEqualTo(1);
      JsonNode a = row("la", d);
      assertThat(a.get("unread").asInt()).isZero();   // own messages are read (spec §3.1)
      assertThat(a.get("peer").get("readSeq").asLong()).isEqualTo(2);
      assertThat(a.get("peer").get("deliveredSeq").asLong()).isEqualTo(2);
    });
  }

  @Test
  void stateSinceReturnsChangedConversations() throws Exception {
    String g1 = group("ta", List.of("tb"));
    String g2 = group("ta", List.of("tc"));
    s.send("ta", g1, "x");
    s.send("ta", g2, "y");
    Thread.sleep(1_500);
    JsonNode first = s.json(s.call("GET", "/v1/state", "ta", null));
    long now = first.get("now").asLong();
    assertThat(first.get("conversations")).hasSizeGreaterThanOrEqualTo(2);
    Thread.sleep(50);
    s.send("tb", g1, "z");
    await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
      JsonNode changed = s.json(s.call("GET", "/v1/state?since=" + now, "ta", null));
      // "now" is 30 s early (final review I5): overlap is allowed, the new state of g1 must be there
      JsonNode c1 = null;
      for (JsonNode c : changed.get("conversations")) {
        if (c.get("id").asText().equals(g1)) {
          c1 = c;
        }
      }
      assertThat(c1).isNotNull();
      assertThat(c1.get("lastSeq").asLong()).isEqualTo(2);
      assertThat(c1.get("unread").asInt()).isEqualTo(1);
      assertThat(changed.get("now").asLong()).isGreaterThan(now);
    });
  }

  @Test
  void receiptsOfLargeGroupIsACount() {
    String g = group("cr0", crowd.subList(1, 60));
    s.send("cr0", g, "hello");
    for (String u : List.of("cr1", "cr2", "cr3")) {
      read(u, g, 1);
    }
    await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(1_100)).untilAsserted(() -> {
      HttpResponse<String> res = s.call("GET", "/v1/conversations/" + g + "/receipts?seq=1", "cr5", null);
      assertThat(res.statusCode()).as(res.body()).isEqualTo(200);   // one lookup per second (spec §3.4)
      JsonNode r = s.json(res);
      assertThat(r.get("readCount").asInt()).isEqualTo(3);
      assertThat(r.has("marks")).isFalse();
    });
    HttpResponse<String> ch = s.call("POST", "/v1/conversations", "cr0", Map.of("kind", "channel", "title", "c",
        "public", true));
    String channel = s.json(ch).get("id").asText();
    HttpResponse<String> r = s.call("GET", "/v1/conversations/" + channel + "/receipts?seq=1", "cr0", null);
    assertThat(r.statusCode()).isEqualTo(404);
    assertThat(s.json(r).get("code").asText()).isEqualTo("not_supported");
    assertThat(s.call("GET", "/v1/conversations/" + g + "/receipts?seq=1", "la", null).statusCode())
        .isEqualTo(403);
  }

  /** Review Focus 3. */
  @Test
  void hideReadNeverLeaksThroughPeerOrCount() throws Exception {
    assertThat(s.call("PUT", "/v1/me/privacy", "hb", Map.of("hideRead", true)).statusCode()).isEqualTo(204);
    assertThat(s.json(s.call("GET", "/v1/me/privacy", "hb", null)).get("hideRead").asBoolean()).isTrue();
    String d = dm("ha", "hb");
    s.send("ha", d, "1");
    read("hb", d, 1);
    String g = group("va", new ArrayList<>(List.of("hb", "vb")));
    s.send("va", g, "g1");
    read("hb", g, 1);
    read("vb", g, 1);
    long t0 = System.currentTimeMillis() - 60_000;
    await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(1_100)).untilAsserted(() -> {
      JsonNode a = row("ha", d);
      // review 2026-11-19 (F3): hb only read (no chat.delivered), so its delivered mark would be its read mark
      assertThat(a.get("peer").get("deliveredSeq").asLong()).isZero();
      assertThat(a.get("peer").get("readSeq").asLong()).isZero();
      for (JsonNode c : s.json(s.call("GET", "/v1/state?since=" + t0, "ha", null)).get("conversations")) {
        if (c.get("id").asText().equals(d)) {
          assertThat(c.get("peer").get("readSeq").asLong()).isZero();
        }
      }
      HttpResponse<String> res = s.call("GET", "/v1/conversations/" + g + "/receipts?seq=1", "va", null);
      assertThat(res.statusCode()).as(res.body()).isEqualTo(200);
      JsonNode marks = s.json(res).get("marks");
      List<String> readers = new ArrayList<>();
      marks.forEach(m -> {
        if (m.get("readSeq").asLong() >= 1) {
          readers.add(m.get("userId").asText());
        }
      });
      assertThat(readers).containsExactly("vb");
      // hb's own unread is right (S4)
      assertThat(row("hb", d).get("unread").asInt()).isZero();
    });
    // reciprocal: hb hides reads, so hb sees nobody's
    JsonNode seen = s.json(s.call("GET", "/v1/conversations/" + g + "/receipts?seq=1", "hb", null)).get("marks");
    seen.forEach(m -> assertThat(m.get("readSeq").asLong()).isZero());
  }

  /** Review 2026-11-19 (F1): a member added later, or a follower joining, does not inherit the history as unread. */
  @Test
  void newMembersStartWithNothingUnread() {
    String g = group("ja", List.of("jb"));
    for (int i = 0; i < 3; i++) {
      s.send("ja", g, "before " + i);
    }
    HttpResponse<String> ch = s.call("POST", "/v1/conversations", "ja", Map.of("kind", "channel", "title", "c",
        "public", true));
    String c = s.json(ch).get("id").asText();
    for (int i = 0; i < 3; i++) {
      s.send("ja", c, "post " + i);
    }
    await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
      assertThat(row("jb", g).get("lastSeq").asLong()).isEqualTo(3);
      assertThat(row("ja", c).get("lastSeq").asLong()).isEqualTo(3);
    });
    assertThat(s.call("POST", "/v1/conversations/" + g + "/members", "ja", Map.of("userId", "jc")).statusCode())
        .isEqualTo(204);
    assertThat(s.call("POST", "/v1/conversations/" + c + "/join", "jd", null).statusCode()).isEqualTo(204);
    assertThat(row("jc", g).get("unread").asInt()).isZero();
    assertThat(row("jd", c).get("unread").asInt()).isZero();
    s.send("ja", g, "after");
    s.send("ja", c, "after");
    await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
      assertThat(row("jc", g).get("unread").asInt()).isEqualTo(1);
      assertThat(row("jd", c).get("unread").asInt()).isEqualTo(1);
    });
  }

  /**
   * Review 2026-11-19 (F3): with reads hidden, the other side sees only delivered marks that came from a real
   * chat.delivered, never a read showing up as "delivered".
   */
  @Test
  void hiddenReadsDoNotShowAsDelivered() {
    assertThat(s.call("PUT", "/v1/me/privacy", "xb", Map.of("hideRead", true)).statusCode()).isEqualTo(204);
    String d = dm("xa", "xb");
    s.send("xa", d, "1");
    s.send("xa", d, "2");
    read("xb", d, 2);
    await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
      assertThat(row("xb", d).get("readSeq").asLong()).isEqualTo(2);   // the read is stored (conv_marks first)
      assertThat(row("xb", d).get("unread").asInt()).isZero();
    });
    assertThat(row("xa", d).get("peer").get("deliveredSeq").asLong()).isZero();
    assertThat(s.call("POST", "/v1/conversations/" + d + "/delivered", "xb", Map.of("upToSeq", 1)).statusCode())
        .isEqualTo(204);
    await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
        assertThat(row("xa", d).get("peer").get("deliveredSeq").asLong()).isEqualTo(1));
  }
}
