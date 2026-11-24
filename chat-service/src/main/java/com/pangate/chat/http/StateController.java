package com.pangate.chat.http;

import com.pangate.chat.core.ApiLimits;
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
  /** {@code hidePreview}: CH4 spec §3.4, notifications without the sender or the text. */
  /** {@code whoCanAdd}: review 2026-11-19 (F7), {@code everyone} or {@code nobody}; null = unchanged. */
  public record PrivacyBody(Boolean hidePresence, Boolean hideRead, Boolean hidePreview, String whoCanAdd) {}

  private final StateIngest state;
  private final com.pangate.chat.core.StateService views;
  private final com.pangate.chat.core.ChatStores.SettingsStore settings;
  private final com.pangate.chat.core.PrivacyCache privacy;
  private final com.pangate.chat.core.PresenceWatch presence;
  private final ApiLimits limits;

  static final int MAX_PRESENCE_IDS = 100;

  public StateController(StateIngest state, com.pangate.chat.core.StateService views,
      com.pangate.chat.core.ChatStores.SettingsStore settings, com.pangate.chat.core.PrivacyCache privacy,
      com.pangate.chat.core.PresenceWatch presence, ApiLimits limits) {
    this.limits = limits;
    this.presence = presence;
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
    limits.check(ApiLimits.Bucket.STATE, AuthFilter.caller(req).userId());
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
    limits.check(ApiLimits.Bucket.OTHER, AuthFilter.caller(req).userId());
    return views.receipts(AuthFilter.caller(req).userId(), id, seq);
  }

  /** CH3 spec §6.3: presence for bots and screens without WebSocket; ids sharing {@code conversationId} only. */
  @GetMapping("/v1/presence")
  public Map<String, Object> presence(@RequestParam("ids") String ids,
      @RequestParam("conversationId") String conversationId, HttpServletRequest req) {
    java.util.List<String> list = java.util.Arrays.stream(ids.split(",")).map(String::strip)
        .filter(x -> !x.isEmpty()).toList();
    if (list.isEmpty() || list.size() > MAX_PRESENCE_IDS) {
      throw new ChatHttpException(400, "invalid_request", "1 to " + MAX_PRESENCE_IDS + " ids");
    }
    limits.check(ApiLimits.Bucket.PRESENCE, AuthFilter.caller(req).userId());
    java.util.List<Map<String, Object>> out = new java.util.ArrayList<>();
    for (var st : presence.states(AuthFilter.caller(req).userId(), conversationId, list)) {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("userId", st.getUserId());
      m.put("online", st.getOnline());
      m.put("lastSeenMs", st.getLastSeenMs());
      m.put("recently", st.getRecently());
      m.put("hidden", st.getHidden());
      out.add(m);
    }
    return Map.of("states", out);
  }

  /** CH3 spec §8. */
  @GetMapping("/v1/me/privacy")
  public Map<String, Object> getPrivacy(HttpServletRequest req) {
    limits.check(ApiLimits.Bucket.OTHER, AuthFilter.caller(req).userId());
    var p = settings.privacy(AuthFilter.caller(req).userId());
    return Map.of("hidePresence", p.hidePresence(), "hideRead", p.hideRead(), "hidePreview", p.hidePreview(),
        "whoCanAdd", p.refuseAdds() ? "nobody" : "everyone");
  }

  @PutMapping(path = "/v1/me/privacy", consumes = "application/json")
  public ResponseEntity<Void> putPrivacy(@RequestBody PrivacyBody body, HttpServletRequest req) {
    if (body == null) {
      throw new ChatHttpException(400, "invalid_request", "a JSON object is required");
    }
    Boolean refuseAdds = switch (body.whoCanAdd() == null ? "" : body.whoCanAdd()) {
      case "" -> null;
      case "everyone" -> false;
      case "nobody" -> true;
      default -> throw new ChatHttpException(400, "invalid_request", "whoCanAdd is everyone or nobody");
    };
    String user = AuthFilter.caller(req).userId();
    limits.check(ApiLimits.Bucket.OTHER, user);
    // Codex review of PR #6: only the fields given are written, with no read first, so concurrent PUTs both stick
    settings.updatePrivacy(user, body.hidePresence(), body.hideRead(), body.hidePreview(), refuseAdds,
        System.currentTimeMillis());
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
