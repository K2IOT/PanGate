package com.pangate.chat.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.pangate.chat.auth.ConnectTokenVerifier;
import com.pangate.chat.auth.InvalidTokenException;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Role;
import com.pangate.chat.core.FakeStores;
import com.pangate.chat.core.Ids;
import com.pangate.chat.core.Ingest;
import com.pangate.contracts.v1.ChatCommand;
import com.pangate.contracts.v1.ConversationKind;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class SendControllerTest {

  final FakeStores stores = new FakeStores();
  final List<ChatCommand> published = new ArrayList<>();
  final Ingest ingest = new Ingest(stores, stores, stores, published::add,
      FakeStores.LIMITS, () -> 0L, System::currentTimeMillis);   // frozen: no refill
  /** Token "u1" is user u1 on device d1; anything else is invalid. */
  final MockMvc mvc;
  final String conv = Ids.next();

  SendControllerTest() {
    stores.create(new Conversation(conv, ConversationKind.CONVERSATION_KIND_GROUP, "g", "u1", 0, false, 1),
        List.of(new Member("u1", Role.MEMBER)));
    AuthFilter auth = new AuthFilter(token -> {
      if (!token.equals("u1")) {
        throw new InvalidTokenException("bad");
      }
      return new ConnectTokenVerifier.Caller("u1", "d1", "J1");
    });
    mvc = MockMvcBuilders.standaloneSetup(new SendController(ingest, stores, com.pangate.chat.core.SendResults.NONE,
        "http:test", java.time.Duration.ZERO)).setControllerAdvice(new ProblemHandler())
        .addFilters(auth).build();
  }

  @Test
  void validSendIs202WithClientMsgId() throws Exception {
    String key = Ids.next();
    mvc.perform(post("/v1/conversations/" + conv + "/messages").header("Authorization", "Bearer u1")
        .header("Idempotency-Key", key).contentType(MediaType.APPLICATION_JSON).content("{\"text\":\"hi\"}"))
        .andExpect(status().isAccepted()).andExpect(jsonPath("$.clientMsgId").value(key));
    assertThat(published).singleElement().satisfies(c -> {
      assertThat(c.getSend().getConversationId()).isEqualTo(conv);
      assertThat(c.getSend().getClientMsgId()).isEqualTo(key);
      assertThat(c.getSenderId()).isEqualTo("u1");
      assertThat(c.getSenderDeviceId()).isEqualTo("d1");
      assertThat(c.getReplyTo()).isEqualTo("http:test");
    });
  }

  @Test
  void aKeyAlreadySequencedIs201WithoutANewCommand() throws Exception {
    String key = Ids.next();
    stores.put(new com.pangate.chat.core.ChatStores.StoredMessage(conv, 7, "01JD0M5G00000000000000000M", "u1", key,
        "", "", List.of(), 1234));
    mvc.perform(post("/v1/conversations/" + conv + "/messages").header("Authorization", "Bearer u1")
        .header("Idempotency-Key", key).contentType(MediaType.APPLICATION_JSON).content("{\"text\":\"hi\"}"))
        .andExpect(status().isCreated()).andExpect(jsonPath("$.convSeq").value(7))
        .andExpect(jsonPath("$.msgId").value("01JD0M5G00000000000000000M")).andExpect(jsonPath("$.sentAt").value(1234));
    assertThat(published).isEmpty();
  }

  @Test
  void errorsAreProblems() throws Exception {
    mvc.perform(post("/v1/conversations/" + conv + "/messages").contentType(MediaType.APPLICATION_JSON)
        .header("Idempotency-Key", Ids.next()).content("{\"text\":\"hi\"}"))
        .andExpect(status().isUnauthorized()).andExpect(jsonPath("$.code").value("invalid_token"));
    mvc.perform(post("/v1/conversations/" + conv + "/messages").header("Authorization", "Bearer u1")
        .contentType(MediaType.APPLICATION_JSON).content("{\"text\":\"hi\"}"))
        .andExpect(status().isBadRequest()).andExpect(jsonPath("$.code").value("invalid_request"));
    mvc.perform(post("/v1/conversations/" + conv + "/messages").header("Authorization", "Bearer u1")
        .header("Idempotency-Key", Ids.next()).contentType(MediaType.APPLICATION_JSON)
        .content("{\"text\":\"" + "a".repeat(4097) + "\"}"))
        .andExpect(status().isBadRequest()).andExpect(jsonPath("$.code").value("invalid_request"));
    mvc.perform(post("/v1/conversations/" + Ids.next() + "/messages").header("Authorization", "Bearer u1")
        .header("Idempotency-Key", Ids.next()).contentType(MediaType.APPLICATION_JSON).content("{\"text\":\"hi\"}"))
        .andExpect(status().isNotFound()).andExpect(jsonPath("$.code").value("not_found"));
    stores.members.get(conv).clear();
    stores.cache.clear();
    mvc.perform(post("/v1/conversations/" + conv + "/messages").header("Authorization", "Bearer u1")
        .header("Idempotency-Key", Ids.next()).contentType(MediaType.APPLICATION_JSON).content("{\"text\":\"hi\"}"))
        .andExpect(status().isForbidden()).andExpect(jsonPath("$.code").value("forbidden"));
    stores.fail = true;
    mvc.perform(post("/v1/conversations/" + conv + "/messages").header("Authorization", "Bearer u1")
        .header("Idempotency-Key", Ids.next()).contentType(MediaType.APPLICATION_JSON).content("{\"text\":\"hi\"}"))
        .andExpect(status().isServiceUnavailable()).andExpect(jsonPath("$.code").value("unavailable"))
        .andExpect(header().exists("Retry-After"));
    assertThat(published).isEmpty();
  }

  @Test
  void rateLimitedIs429() throws Exception {
    int last = 0;
    for (int i = 0; i < 41; i++) {
      last = mvc.perform(post("/v1/conversations/" + conv + "/messages").header("Authorization", "Bearer u1")
          .header("Idempotency-Key", Ids.next()).contentType(MediaType.APPLICATION_JSON).content("{\"text\":\"hi\"}"))
          .andReturn().getResponse().getStatus();
    }
    assertThat(last).isEqualTo(429);
  }
  /** Final review minor: only a Kafka that did not ack is 503; any other IllegalStateException is a bug (500). */
  @Test
  void kafkaWithoutAckIs503AndOtherIllegalStateIsNot() throws Exception {
    MockMvc queueDown = mvcWithSink(c -> {
      throw new com.pangate.chat.core.QueueUnavailableException("chat.commands not acked", null);
    });
    queueDown.perform(post("/v1/conversations/" + conv + "/messages").header("Authorization", "Bearer u1")
        .header("Idempotency-Key", Ids.next()).contentType(MediaType.APPLICATION_JSON).content("{\"text\":\"hi\"}"))
        .andExpect(status().isServiceUnavailable()).andExpect(header().exists("Retry-After"));
    MockMvc bug = mvcWithSink(c -> {
      throw new IllegalStateException("a bug");
    });
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> bug.perform(post("/v1/conversations/" + conv
        + "/messages").header("Authorization", "Bearer u1").header("Idempotency-Key", Ids.next())
        .contentType(MediaType.APPLICATION_JSON).content("{\"text\":\"hi\"}")))
        .hasRootCauseInstanceOf(IllegalStateException.class);
  }

  private MockMvc mvcWithSink(com.pangate.chat.core.CommandSink sink) {
    Ingest failing = new Ingest(stores, stores, stores, sink, FakeStores.LIMITS, () -> 0L, System::currentTimeMillis);
    return MockMvcBuilders.standaloneSetup(new SendController(failing, stores,
        com.pangate.chat.core.SendResults.NONE, "http:test", java.time.Duration.ZERO))
        .setControllerAdvice(new ProblemHandler()).addFilters(new AuthFilter(token -> new ConnectTokenVerifier.Caller(
            "u1", "d1", "J1"))).build();
  }
}
