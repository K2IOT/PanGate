package com.pangate.chat.http;

import static org.assertj.core.api.Assertions.assertThat;
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
    mvc = MockMvcBuilders.standaloneSetup(new StateController(state, new com.pangate.chat.core.StateService(stores,
        new MemberLookup(stores, stores), stores, stores, stores, privacy, 50, () -> 0L, System::currentTimeMillis),
        stores, privacy)).setControllerAdvice(new ProblemHandler())
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
}
