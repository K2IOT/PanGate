package com.pangate.chat.http;

import com.pangate.chat.core.MarkValidator;
import com.pangate.chat.core.StateIngest;
import com.pangate.contracts.v1.ChatMark;
import jakarta.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** CH3 conversation-state API (spec §3.3; the rest of §7–§8 is added by later parts of CH3). */
@RestController
public class StateController {

  /** {@code POST .../read} and {@code .../delivered} body. */
  public record MarkBody(Long upToSeq) {}

  /** {@code PUT /v1/me/privacy} body; a missing field keeps its value. */
  public record PrivacyBody(Boolean hidePresence, Boolean hideRead) {}

  private final StateIngest state;
  private final com.pangate.chat.core.StateService views;
  private final com.pangate.chat.core.ChatStores.SettingsStore settings;
  private final com.pangate.chat.core.PrivacyCache privacy;

  public StateController(StateIngest state, com.pangate.chat.core.StateService views,
      com.pangate.chat.core.ChatStores.SettingsStore settings, com.pangate.chat.core.PrivacyCache privacy) {
    this.state = state;
    this.views = views;
    this.settings = settings;
    this.privacy = privacy;
  }

  /** CH3 spec §7.2: conversations whose state changed after {@code since} (ms), or all of them. */
  @GetMapping("/v1/state")
  public Map<String, Object> state(@RequestParam(name = "since", required = false) Long since,
      @RequestParam(name = "cursor", required = false) String cursor, HttpServletRequest req) {
    if (cursor != null && !cursor.isEmpty() && !cursor.matches("\\d{1,19}:[0-9A-Z]{26}")) {
      throw new ChatHttpException(400, "invalid_request", "bad cursor");
    }
    var page = views.state(AuthFilter.caller(req).userId(), since == null ? OptionalLong.empty()
        : OptionalLong.of(since), cursor);
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("conversations", page.conversations());
    body.put("cursor", page.cursor());
    body.put("now", page.now());
    return body;
  }

  /** CH3 spec §3.4. */
  @GetMapping("/v1/conversations/{id}/receipts")
  public Map<String, Object> receipts(@PathVariable("id") String id, @RequestParam("seq") long seq,
      HttpServletRequest req) {
    if (seq < 1) {
      throw new ChatHttpException(400, "invalid_request", "seq must be at least 1");
    }
    return views.receipts(AuthFilter.caller(req).userId(), id, seq);
  }

  /** CH3 spec §8. */
  @GetMapping("/v1/me/privacy")
  public Map<String, Object> getPrivacy(HttpServletRequest req) {
    var p = settings.privacy(AuthFilter.caller(req).userId());
    return Map.of("hidePresence", p.hidePresence(), "hideRead", p.hideRead());
  }

  @PutMapping(path = "/v1/me/privacy", consumes = "application/json")
  public ResponseEntity<Void> putPrivacy(@RequestBody PrivacyBody body, HttpServletRequest req) {
    if (body == null) {
      throw new ChatHttpException(400, "invalid_request", "a JSON object is required");
    }
    String user = AuthFilter.caller(req).userId();
    var cur = settings.privacy(user);
    settings.putPrivacy(user, new com.pangate.chat.core.ChatStores.Privacy(
        body.hidePresence() == null ? cur.hidePresence() : body.hidePresence(),
        body.hideRead() == null ? cur.hideRead() : body.hideRead()), System.currentTimeMillis());
    privacy.forget(user);
    return ResponseEntity.noContent().build();
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
