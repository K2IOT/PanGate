package com.pangate.chat.http;

import com.pangate.chat.auth.ConnectTokenVerifier.Caller;
import com.pangate.chat.core.ChatRefusal;
import com.pangate.chat.core.ChatStores;
import com.pangate.chat.core.ChatStores.Conversation;
import com.pangate.chat.core.ChatStores.StoredMessage;
import com.pangate.chat.core.ChatStores.UserConv;
import com.pangate.chat.core.Conversations;
import com.pangate.chat.core.SyncService;
import com.pangate.chat.core.SyncService.SyncCursor;
import com.pangate.chat.core.SyncService.SyncResult;
import com.pangate.contracts.v1.ConversationKind;
import jakarta.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Reads and conversation management (chat spec §7); the user is the connect token's. */
@RestController
public class QueryController {

  static final int MAX_PAGE = 100;
  static final int DEFAULT_PAGE = 50;

  private final Conversations conversations;
  private final SyncService sync;
  private final ChatStores.MemberStore members;
  private final ChatStores.MessageStore messages;

  public QueryController(Conversations conversations, SyncService sync, ChatStores.MemberStore members,
      ChatStores.MessageStore messages) {
    this.conversations = conversations;
    this.sync = sync;
    this.members = members;
    this.messages = messages;
  }

  static String userId(HttpServletRequest req) {
    Caller c = AuthFilter.caller(req);
    return c.userId();
  }

  static int page(Integer limit) {
    if (limit == null) {
      return DEFAULT_PAGE;
    }
    if (limit < 1 || limit > MAX_PAGE) {
      throw new ChatRefusal(ChatRefusal.INVALID, "limit is 1.." + MAX_PAGE);
    }
    return limit;
  }

  static String kindName(ConversationKind k) {
    return switch (k) {
      case CONVERSATION_KIND_DIRECT -> "direct";
      case CONVERSATION_KIND_GROUP -> "group";
      case CONVERSATION_KIND_CHANNEL -> "channel";
      default -> "unknown";
    };
  }

  static ConversationKind kind(Object name) {
    if ("group".equals(name)) {
      return ConversationKind.CONVERSATION_KIND_GROUP;
    }
    if ("channel".equals(name)) {
      return ConversationKind.CONVERSATION_KIND_CHANNEL;
    }
    throw new ChatRefusal(ChatRefusal.INVALID, "kind must be group or channel");
  }

  Map<String, Object> conversation(Conversation c) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("id", c.id());
    m.put("kind", kindName(c.kind()));
    m.put("title", c.title());
    m.put("createdBy", c.createdBy());
    m.put("lastSeq", c.lastSeq());
    m.put("public", c.isPublic());
    if (c.kind() != ConversationKind.CONVERSATION_KIND_CHANNEL) {
      m.put("memberCount", members.members(c.id()).size());   // ≤ 500; a channel's followers are not counted
    }
    return m;
  }

  static Map<String, Object> message(StoredMessage s) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("msgId", s.msgId());
    m.put("convSeq", s.convSeq());
    m.put("senderId", s.senderId());
    m.put("clientMsgId", s.clientMsgId());
    m.put("text", s.text());
    m.put("replyToMsgId", s.replyTo());
    m.put("mentionUserIds", s.mentions());
    m.put("sentAt", s.sentAtMs());
    return m;
  }

  @GetMapping("/v1/conversations")
  public Map<String, Object> list(@RequestParam(name = "cursor", required = false) String cursor,
      @RequestParam(name = "limit", required = false) Integer limit, HttpServletRequest req) {
    int n = page(limit);
    if (cursor != null && !cursor.isEmpty() && !cursor.matches("\\d{1,19}:[0-9A-Z]{26}")) {
      throw new ChatRefusal(ChatRefusal.INVALID, "bad cursor");
    }
    List<UserConv> page = members.userConvs(userId(req), cursor, n);
    List<Map<String, Object>> out = new ArrayList<>();
    for (UserConv u : page) {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("id", u.convId());
      m.put("kind", kindName(u.kind()));
      m.put("lastSeq", u.lastSeq());
      m.put("lastMsgAt", u.lastMsgAtMs());
      out.add(m);
    }
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("conversations", out);
    body.put("nextCursor", page.size() == n ? page.getLast().lastMsgAtMs() + ":" + page.getLast().convId() : null);
    return body;
  }

  @GetMapping("/v1/conversations/{id}")
  public Map<String, Object> get(@PathVariable("id") String id, HttpServletRequest req) {
    return conversation(conversations.visible(userId(req), id));
  }

  @GetMapping("/v1/conversations/{id}/messages")
  public Map<String, Object> history(@PathVariable("id") String id,
      @RequestParam(name = "before", required = false) Long before,
      @RequestParam(name = "after", required = false) Long after,
      @RequestParam(name = "limit", required = false) Integer limit, HttpServletRequest req) {
    int n = page(limit);
    if (before != null && after != null) {
      throw new ChatRefusal(ChatRefusal.INVALID, "before or after, not both");
    }
    if ((before != null && before < 0) || (after != null && after < 0)) {
      throw new ChatRefusal(ChatRefusal.INVALID, "before / after are >= 0");
    }
    Conversation c = conversations.visible(userId(req), id);
    List<StoredMessage> ms = after != null ? messages.after(id, after, n)
        : messages.before(id, Math.min(before == null ? Long.MAX_VALUE : before, c.lastSeq() + 1), n);
    return Map.of("messages", ms.stream().map(QueryController::message).toList(), "lastSeq", c.lastSeq());
  }

  @PostMapping(path = "/v1/sync", consumes = "application/json")
  public Map<String, Object> sync(@RequestBody Map<String, Object> body, HttpServletRequest req) {
    if (!(body.get("conversations") instanceof List<?> list)) {
      throw new ChatRefusal(ChatRefusal.INVALID, "conversations must be an array");
    }
    List<SyncCursor> in = new ArrayList<>();
    for (Object o : list) {
      if (!(o instanceof Map<?, ?> m) || !(m.get("id") instanceof String cid) || !(m.get("lastSeq") instanceof Number s)) {
        throw new ChatRefusal(ChatRefusal.INVALID, "each conversation is {id, lastSeq}");
      }
      in.add(new SyncCursor(cid, s.longValue()));
    }
    Object per = body.get("limitPerConversation");
    if (per != null && !(per instanceof Number)) {
      throw new ChatRefusal(ChatRefusal.INVALID, "limitPerConversation must be a number");
    }
    int perConv = per == null ? 0 : ((Number) per).intValue();
    if (per != null && perConv == 0) {
      throw new ChatRefusal(ChatRefusal.INVALID, "limitPerConversation is 1.." + SyncService.MAX_PER_CONV);
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (SyncResult r : sync.sync(userId(req), in, perConv)) {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("id", r.convId());
      m.put("messages", r.messages().stream().map(QueryController::message).toList());
      m.put("tooMany", r.tooMany());
      m.put("lastSeq", r.lastSeq());
      out.add(m);
    }
    return Map.of("conversations", out);
  }

  @PostMapping(path = "/v1/conversations", consumes = "application/json")
  public ResponseEntity<Map<String, Object>> create(@RequestBody Map<String, Object> body, HttpServletRequest req) {
    Object title = body.get("title");
    if (title != null && !(title instanceof String)) {
      throw new ChatRefusal(ChatRefusal.INVALID, "title must be a string");
    }
    Object ids = body.get("memberIds");
    if (ids != null && (!(ids instanceof List<?> l) || !l.stream().allMatch(String.class::isInstance))) {
      throw new ChatRefusal(ChatRefusal.INVALID, "memberIds must be an array of strings");
    }
    Object pub = body.get("public");
    if (pub != null && !(pub instanceof Boolean)) {
      throw new ChatRefusal(ChatRefusal.INVALID, "public must be a boolean");
    }
    @SuppressWarnings("unchecked")
    List<String> memberIds = ids == null ? List.of() : (List<String>) ids;
    Conversation c = conversations.create(userId(req), kind(body.get("kind")), (String) title, memberIds,
        Boolean.TRUE.equals(pub));
    return ResponseEntity.status(201).body(conversation(c));
  }

  @PostMapping("/v1/dm/{userId}")
  public Map<String, Object> dm(@PathVariable("userId") String other, HttpServletRequest req) {
    return conversation(conversations.openDm(userId(req), other));
  }

  @PostMapping(path = "/v1/conversations/{id}/members", consumes = "application/json")
  public ResponseEntity<Void> addMember(@PathVariable("id") String id, @RequestBody Map<String, Object> body,
      HttpServletRequest req) {
    if (!(body.get("userId") instanceof String u)) {
      throw new ChatRefusal(ChatRefusal.INVALID, "userId must be a string");
    }
    conversations.addMember(userId(req), id, u);
    return ResponseEntity.noContent().build();
  }

  @DeleteMapping("/v1/conversations/{id}/members/{userId}")
  public ResponseEntity<Void> removeMember(@PathVariable("id") String id, @PathVariable("userId") String user,
      HttpServletRequest req) {
    conversations.removeMember(userId(req), id, user);
    return ResponseEntity.noContent().build();
  }

  @PostMapping("/v1/conversations/{id}/join")
  public ResponseEntity<Void> join(@PathVariable("id") String id, HttpServletRequest req) {
    conversations.join(userId(req), id);
    return ResponseEntity.noContent().build();
  }
}
