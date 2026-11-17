package com.pangate.chat.http;

import com.pangate.chat.core.MarkValidator;
import com.pangate.chat.core.StateIngest;
import com.pangate.contracts.v1.ChatMark;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** CH3 conversation-state API (spec §3.3; the rest of §7–§8 is added by later parts of CH3). */
@RestController
public class StateController {

  /** {@code POST .../read} and {@code .../delivered} body. */
  public record MarkBody(Long upToSeq) {}

  private final StateIngest state;

  public StateController(StateIngest state) {
    this.state = state;
  }

  @PostMapping(path = "/v1/conversations/{id}/read", consumes = "application/json")
  public ResponseEntity<Void> read(@PathVariable("id") String id, @RequestBody MarkBody body, HttpServletRequest req) {
    return mark(id, body, true, req);
  }

  @PostMapping(path = "/v1/conversations/{id}/delivered", consumes = "application/json")
  public ResponseEntity<Void> delivered(@PathVariable("id") String id, @RequestBody MarkBody body,
      HttpServletRequest req) {
    return mark(id, body, false, req);
  }

  private ResponseEntity<Void> mark(String id, MarkBody body, boolean read, HttpServletRequest req) {
    if (body == null || body.upToSeq() == null) {
      throw new ChatHttpException(400, "invalid_request", "upToSeq is required");
    }
    var caller = AuthFilter.caller(req);
    String reason = state.markHttp(caller.userId(), caller.deviceId(),
        ChatMark.newBuilder().setConversationId(id).setUpToSeq(Math.max(0, body.upToSeq())).build(), read);
    return switch (reason) {
      case "" -> ResponseEntity.noContent().build();
      case MarkValidator.NOT_MEMBER -> throw new ChatHttpException(403, "forbidden", "not a member");
      case MarkValidator.RATE_LIMITED -> throw new ChatHttpException(429, "rate_limited", "too many marks");
      default -> throw new ChatHttpException(400, "invalid_request", "bad conversation id or upToSeq");
    };
  }
}
