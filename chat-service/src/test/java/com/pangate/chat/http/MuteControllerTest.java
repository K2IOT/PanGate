package com.pangate.chat.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.pangate.chat.auth.ConnectTokenVerifier;
import com.pangate.chat.auth.InvalidTokenException;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.contracts.v1.ConversationKind;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/** CH4 spec §3.3: PUT /v1/conversations/{id}/mute. */
class MuteControllerTest {

  final FakeStores stores = new FakeStores();
  final String conv = Ids.next();
  final MockMvc mvc;

  MuteControllerTest() {
    stores.create(new Conversation(conv, ConversationKind.CONVERSATION_KIND_GROUP, "g", "u1", 0, false, 1),
        List.of(new Member("u1", Role.MEMBER)));
    AuthFilter auth = new AuthFilter(token -> {
      if (!token.startsWith("u")) {
        throw new InvalidTokenException("bad");
      }
      return new ConnectTokenVerifier.Caller(token, "d1", "J1");
    });
    mvc = MockMvcBuilders.standaloneSetup(new MuteController(stores, stores, () -> 1_800_000_000_000L,
        new com.pangate.chat.core.ApiLimits(com.pangate.chat.config.ChatProperties.Api.DEFAULTS, () -> 0L))).setControllerAdvice(new ProblemHandler()).addFilters(auth).build();
  }

  org.springframework.test.web.servlet.ResultActions mute(String user, String c, String body) throws Exception {
    return mvc.perform(put("/v1/conversations/" + c + "/mute").header("Authorization", "Bearer " + user)
        .contentType(MediaType.APPLICATION_JSON).content(body));
  }

  @Test
  void nullMutesForEver() throws Exception {
    mute("u1", conv, "{\"until\":null}").andExpect(status().isNoContent());
    assertThat(stores.mutedUntil(conv, "u1")).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void aTimeMutesUntilThenAPastTimeUnmutes() throws Exception {
    mute("u1", conv, "{\"until\":1800003600000}").andExpect(status().isNoContent());
    assertThat(stores.mutedUntil(conv, "u1")).isEqualTo(1_800_003_600_000L);
    mute("u1", conv, "{\"until\":1}").andExpect(status().isNoContent());
    assertThat(stores.mutedUntil(conv, "u1")).isZero();
  }

  @Test
  void notAMemberIs403() throws Exception {
    mute("u9", conv, "{\"until\":null}").andExpect(status().isForbidden())
        .andExpect(jsonPath("$.code").value("forbidden"));
  }

  @Test
  void badConversationIs400() throws Exception {
    mute("u1", "nope", "{\"until\":null}").andExpect(status().isBadRequest());
  }

  /** Perf 3a (spec §3.2): mute shares the "other" bucket, 10/s burst 30. */
  @Test
  void muteIsLimited() throws Exception {
    String c = com.pangate.chat.core.Ids.next();
    for (int i = 0; i < 30; i++) {
      mvc.perform(put("/v1/conversations/" + c + "/mute").header("Authorization", "Bearer ulim")
          .contentType(org.springframework.http.MediaType.APPLICATION_JSON).content("{}"));
    }
    mvc.perform(put("/v1/conversations/" + c + "/mute").header("Authorization", "Bearer ulim")
        .contentType(org.springframework.http.MediaType.APPLICATION_JSON).content("{}"))
        .andExpect(status().isTooManyRequests());
  }
}
