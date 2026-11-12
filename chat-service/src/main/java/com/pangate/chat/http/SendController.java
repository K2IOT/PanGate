package com.pangate.chat.http;

import com.pangate.chat.auth.ConnectTokenVerifier.Caller;
import com.pangate.chat.core.Ingest;
import com.pangate.chat.core.Rejection;
import com.pangate.chat.core.SendInput;
import com.pangate.contracts.v1.ChatSend;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code POST /v1/conversations/{id}/messages} (chat spec §4.2): {@code Idempotency-Key} is the {@code clientMsgId};
 * body {@code {text, replyToMsgId?, mentionUserIds?}}. Same checks as the WebSocket path; an accepted send is
 * {@code 202 {clientMsgId}}.
 */
@RestController
public class SendController {

  private final Ingest ingest;

  public SendController(Ingest ingest) {
    this.ingest = ingest;
  }

  @PostMapping(path = "/v1/conversations/{id}/messages", consumes = "application/json")
  public ResponseEntity<Map<String, Object>> send(@PathVariable("id") String conversationId,
      @RequestHeader(name = "Idempotency-Key", required = false) String key, @RequestBody Map<String, Object> body,
      HttpServletRequest req) {
    if (key == null || key.isBlank()) {
      throw new ChatHttpException(400, "invalid_request", "Idempotency-Key is required");
    }
    Caller caller = AuthFilter.caller(req);
    ChatSend send = toSend(conversationId, key, body);
    Optional<Rejection> r = ingest.accept(new SendInput(caller.userId(), caller.deviceId(), send, ""));
    if (r.isPresent()) {
      throw toHttp(r.get());
    }
    return ResponseEntity.accepted().body(Map.of("clientMsgId", key));
  }

  static ChatSend toSend(String conversationId, String key, Map<String, Object> body) {
    ChatSend.Builder b = ChatSend.newBuilder().setConversationId(conversationId).setClientMsgId(key);
    if (!(body.get("text") instanceof String text)) {
      throw new ChatHttpException(400, "invalid_request", "text must be a string");
    }
    b.setText(text);
    Object reply = body.get("replyToMsgId");
    if (reply != null) {
      if (!(reply instanceof String s)) {
        throw new ChatHttpException(400, "invalid_request", "replyToMsgId must be a string");
      }
      b.setReplyToMsgId(s);
    }
    Object mentions = body.get("mentionUserIds");
    if (mentions != null) {
      if (!(mentions instanceof List<?> list) || !list.stream().allMatch(String.class::isInstance)) {
        throw new ChatHttpException(400, "invalid_request", "mentionUserIds must be an array of strings");
      }
      list.forEach(m -> b.addMentionUserIds((String) m));
    }
    return b.build();
  }

  static ChatHttpException toHttp(Rejection r) {
    return switch (r.reason()) {
      case Rejection.NOT_FOUND -> new ChatHttpException(404, "not_found", r.detail());
      case Rejection.NOT_MEMBER -> new ChatHttpException(403, "forbidden", r.detail());
      case Rejection.RATE_LIMITED -> new ChatHttpException(429, "rate_limited", r.detail());
      default -> new ChatHttpException(400, "invalid_request", r.detail());
    };
  }
}
