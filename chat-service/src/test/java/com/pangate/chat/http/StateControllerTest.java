package com.pangate.chat.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.pangate.chat.auth.ConnectTokenVerifier;
import com.pangate.chat.auth.InvalidTokenException;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.LastSeqCache;
import com.pangate.chat.core.MarkValidator;
import com.pangate.chat.core.MemberLookup;
import com.pangate.chat.core.StateIngest;
import com.pangate.chat.core.TokenBucketLimiter;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.ReceiptCommand;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/** CH3 spec §3.3: POST /v1/conversations/{id}/read and /delivered for bots and HTTP clients. */
class StateControllerTest {

  final FakeStores stores = new FakeStores();
  final List<ReceiptCommand> published = new ArrayList<>();
  final com.pangate.chat.core.FakePresence presence = new com.pangate.chat.core.FakePresence();
  final String conv = Ids.next();
  final MockMvc mvc;

  StateControllerTest() {
    stores.create(new Conversation(conv, ConversationKind.CONVERSATION_KIND_GROUP, "g", "u1", 0, false, 1),
        List.of(new Member("u1", Role.MEMBER)));
    stores.lastSeq.put(conv, 20L);
    AuthFilter auth = new AuthFilter(token -> {
      if (!token.startsWith("u")) {
        throw new InvalidTokenException("bad");
      }
      return new ConnectTokenVerifier.Caller(token, "d1", "J1");
    });
    StateIngest state = new StateIngest(new MarkValidator(new MemberLookup(stores, stores),
        new LastSeqCache(stores, () -> 0L), new TokenBucketLimiter(1, 1, () -> 0L), r -> { }), published::add);
    com.pangate.chat.core.PrivacyCache privacy = new com.pangate.chat.core.PrivacyCache(stores, () -> 0L);
    presence.add("u2", "n1", 1, "c1");
    stores.add(conv, new Member("u2", Role.MEMBER));
    stores.sent(conv, "u2", 1, 0);   // the presence rule: u2 took part
    com.pangate.chat.core.PresenceWatch watch = new com.pangate.chat.core.PresenceWatch(stores,
        new MemberLookup(stores, stores), stores, new com.pangate.chat.core.FakeWatchers(System::currentTimeMillis), presence,
        stores, privacy, (r, prio) -> new java.util.concurrent.CompletableFuture<>(), System::currentTimeMillis,
        java.time.Duration.ofMinutes(5), 200);
    mvc = MockMvcBuilders.standaloneSetup(new StateController(state, new com.pangate.chat.core.StateService(stores,
        new MemberLookup(stores, stores), stores, stores, stores, privacy, 50, () -> 0L, System::currentTimeMillis),
        stores, privacy, watch, com.pangate.chat.core.ApiLimits.unlimited())).setControllerAdvice(new ProblemHandler())
        .addFilters(auth).build();
  }

  @Test
  void readIs204AndPublishes() throws Exception {
    mvc.perform(post("/v1/conversations/" + conv + "/read").header("Authorization", "Bearer u1")
        .contentType(MediaType.APPLICATION_JSON).content("{\"upToSeq\":5}")).andExpect(status().isNoContent());
    assertThat(published).singleElement().satisfies(c -> {
      assertThat(c.getReadSeq()).isEqualTo(5);
      assertThat(c.getUserId()).isEqualTo("u1");
    });
  }

  @Test
  void deliveredDoesNotMoveRead() throws Exception {
    mvc.perform(post("/v1/conversations/" + conv + "/delivered").header("Authorization", "Bearer u1")
        .contentType(MediaType.APPLICATION_JSON).content("{\"upToSeq\":5}")).andExpect(status().isNoContent());
    assertThat(published).singleElement().satisfies(c -> {
      assertThat(c.getDeliveredSeq()).isEqualTo(5);
      assertThat(c.getReadSeq()).isZero();
    });
  }

  @Test
  void refusalsMapToStableCodes() throws Exception {
    mvc.perform(post("/v1/conversations/" + conv + "/read").header("Authorization", "Bearer u9")
        .contentType(MediaType.APPLICATION_JSON).content("{\"upToSeq\":5}")).andExpect(status().isForbidden())
        .andExpect(jsonPath("$.code").value("forbidden"));
    mvc.perform(post("/v1/conversations/" + conv + "/read").header("Authorization", "Bearer u1")
        .contentType(MediaType.APPLICATION_JSON).content("{\"upToSeq\":0}")).andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("invalid_request"));
    mvc.perform(post("/v1/conversations/" + conv + "/read").header("Authorization", "Bearer u1")
        .contentType(MediaType.APPLICATION_JSON).content("{\"upToSeq\":5}")).andExpect(status().isNoContent());
    mvc.perform(post("/v1/conversations/" + conv + "/read").header("Authorization", "Bearer u1")
        .contentType(MediaType.APPLICATION_JSON).content("{\"upToSeq\":6}")).andExpect(status().isTooManyRequests())
        .andExpect(jsonPath("$.code").value("rate_limited"));
  }

  /** CH3 spec §6.3: presence over HTTP, only of users sharing the conversation. */
  @Test
  void presenceOfMembersOnly() throws Exception {
    mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get("/v1/presence?ids=u2,zz&"
        + "conversationId=" + conv).header("Authorization", "Bearer u1")).andExpect(status().isOk())
        .andExpect(jsonPath("$.states.length()").value(1)).andExpect(jsonPath("$.states[0].userId").value("u2"))
        .andExpect(jsonPath("$.states[0].online").value(true));
    String tooMany = String.join(",", java.util.Collections.nCopies(101, "u2"));
    mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get("/v1/presence?ids=" + tooMany
        + "&conversationId=" + conv).header("Authorization", "Bearer u1")).andExpect(status().isBadRequest());
  }

  /** Perf 3a (spec §3.2): GET /v1/presence is limited per user before any lookup. */
  @Test
  void presenceIsLimitedBeforeLookup() throws Exception {
    com.pangate.chat.core.PrivacyCache privacy = new com.pangate.chat.core.PrivacyCache(stores, () -> 0L);
    com.pangate.chat.core.PresenceWatch watch = new com.pangate.chat.core.PresenceWatch(stores,
        new MemberLookup(stores, stores), stores, new com.pangate.chat.core.FakeWatchers(System::currentTimeMillis),
        presence, stores, privacy, (r, prio) -> new java.util.concurrent.CompletableFuture<>(), System::currentTimeMillis,
        java.time.Duration.ofMinutes(5), 200);
    StateIngest state = new StateIngest(new MarkValidator(new MemberLookup(stores, stores),
        new LastSeqCache(stores, () -> 0L), new TokenBucketLimiter(1, 1, () -> 0L), r -> { }), published::add);
    org.springframework.test.web.servlet.MockMvc limited = MockMvcBuilders.standaloneSetup(new StateController(state,
        new com.pangate.chat.core.StateService(stores, new MemberLookup(stores, stores), stores, stores, stores,
            privacy, 50, () -> 0L, System::currentTimeMillis), stores, privacy, watch,
        new com.pangate.chat.core.ApiLimits(com.pangate.chat.config.ChatProperties.Api.DEFAULTS, () -> 0L)))
        .setControllerAdvice(new ProblemHandler()).addFilters(new AuthFilter(t -> new ConnectTokenVerifier.Caller(t,
            "d1", "J1"))).build();
    for (int i = 0; i < 10; i++) {
      limited.perform(get("/v1/presence").param("ids", "u2").param("conversationId", conv)
          .header("Authorization", "Bearer u1")).andExpect(status().isOk());
    }
    int gets = stores.getCalls.get();
    limited.perform(get("/v1/presence").param("ids", "u2").param("conversationId", conv)
        .header("Authorization", "Bearer u1")).andExpect(status().isTooManyRequests());
    assertThat(stores.getCalls.get()).isEqualTo(gets);
  }
}
