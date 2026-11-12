package com.pangate.chat.http;

import com.pangate.chat.auth.ConnectTokenVerifier.Caller;
import com.pangate.chat.core.ChatStores;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.Ingest;
import com.pangate.chat.core.Rejection;
import com.pangate.chat.core.SendInput;
import com.pangate.chat.core.SendResults;
import java.time.Duration;
import com.pangate.contracts.v1.ChatSend;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * {@code POST /v1/conversations/{id}/messages} (chat spec §4.2): {@code Idempotency-Key} is the {@code clientMsgId};
 * body {@code {text, replyToMsgId?, mentionUserIds?}}. Same checks as the WebSocket path. A key already sequenced
 * answers its message at once; otherwise the pod waits up to {@code sendWait} for the sequencer:
 * {@code 201 {msgId, convSeq, sentAt}}, or {@code 202 {clientMsgId}} on timeout or without Redis.
 */
@RestController
public class SendController {

  private final Ingest ingest;
  private final ChatStores.DedupStore prior;
  private final SendResults results;
  private final String replyTo;
  private final Duration sendWait;

  /** {@code prior}: earlier sends (Redis, then ScyllaDB); {@code replyTo}: {@code http:<this pod>}. */
  public SendController(Ingest ingest, @Qualifier("priorSends") ChatStores.DedupStore prior, SendResults results,
      @Qualifier("httpReplyTo") String replyTo, @Qualifier("httpSendWait") Duration sendWait) {
    this.ingest = ingest;
    this.prior = prior;
    this.results = results;
    this.replyTo = replyTo;
    this.sendWait = sendWait;
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
    Optional<StoredMessage> done = prior.find(conversationId, caller.userId(), key);
    if (done.isPresent()) {
      return created(done.get());
    }
    Optional<Rejection> r = ingest.accept(new SendInput(caller.userId(), caller.deviceId(), send, replyTo));
    if (r.isPresent()) {
      throw toHttp(r.get());
    }
    return results.await(SendResults.key(conversationId, caller.userId(), key), sendWait)
        .map(SendController::created)
        .orElseGet(() -> ResponseEntity.accepted().body(Map.of("clientMsgId", key)));
  }

  static ResponseEntity<Map<String, Object>> created(StoredMessage m) {
    return ResponseEntity.status(201).body(Map.of("msgId", m.msgId(), "convSeq", m.convSeq(), "sentAt",
        m.sentAtMs()));
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
