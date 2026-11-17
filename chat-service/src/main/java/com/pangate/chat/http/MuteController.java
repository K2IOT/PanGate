package com.pangate.chat.http;

import com.pangate.chat.core.ChatRefusal;
import com.pangate.chat.core.ChatStores;
import com.pangate.chat.core.Ids;
import jakarta.servlet.http.HttpServletRequest;
import java.util.function.LongSupplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** CH4 spec §3.3: {@code PUT /v1/conversations/{id}/mute {"until": <epoch ms> | null}}. */
@RestController
public class MuteController {

  /** {@code until}: null (or absent) = for ever; a time already past = unmute. */
  public record MuteBody(Long until) {}

  private final ChatStores.MuteStore mutes;
  private final ChatStores.MemberStore members;
  private final LongSupplier wallMs;

  @Autowired
  public MuteController(ChatStores.MuteStore mutes, ChatStores.MemberStore members) {
    this(mutes, members, System::currentTimeMillis);
  }

  MuteController(ChatStores.MuteStore mutes, ChatStores.MemberStore members, LongSupplier wallMs) {
    this.mutes = mutes;
    this.members = members;
    this.wallMs = wallMs;
  }

  @PutMapping(path = "/v1/conversations/{id}/mute", consumes = "application/json")
  public ResponseEntity<Void> mute(@PathVariable("id") String id, @RequestBody(required = false) MuteBody body,
      HttpServletRequest req) {
    if (!Ids.isUlid(id)) {
      throw new ChatRefusal(ChatRefusal.INVALID, "conversation id must be a ULID");
    }
    String user = AuthFilter.caller(req).userId();
    if (members.member(id, user).isEmpty()) {
      throw new ChatRefusal(ChatRefusal.FORBIDDEN, "not a member of this conversation");
    }
    Long until = body == null ? null : body.until();
    long value = until == null ? Long.MAX_VALUE : until <= wallMs.getAsLong() ? 0 : until;
    mutes.setMute(id, user, value);
    return ResponseEntity.noContent().build();
  }
}
