package com.pangate.chat.core;

import com.pangate.chat.config.ChatProperties;
import com.pangate.contracts.v1.ChatSend;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/** The stateless rules of chat spec §4.3; the text limit counts UTF-8 bytes. */
public final class SendValidator {

  private SendValidator() {}

  public static Optional<Rejection> check(ChatSend s, ChatProperties.Limits l) {
    if (!Ids.isUlid(s.getConversationId())) {
      return reject(Rejection.INVALID, "conversation_id must be a ULID");
    }
    if (!Ids.isUlid(s.getClientMsgId())) {
      return reject(Rejection.INVALID, "client_msg_id must be a ULID");
    }
    if (s.getText().isEmpty()) {
      return reject(Rejection.INVALID, "text is empty");
    }
    if (s.getText().getBytes(StandardCharsets.UTF_8).length > l.maxTextBytes()) {
      return reject(Rejection.TOO_LONG, "text is over " + l.maxTextBytes() + " UTF-8 bytes");
    }
    if (s.getMentionUserIdsCount() > l.maxMentions()) {
      return reject(Rejection.INVALID, "at most " + l.maxMentions() + " mentions");
    }
    if (!s.getReplyToMsgId().isEmpty() && !Ids.isUlid(s.getReplyToMsgId())) {
      return reject(Rejection.INVALID, "reply_to_msg_id must be a ULID");
    }
    return Optional.empty();
  }

  private static Optional<Rejection> reject(String reason, String detail) {
    return Optional.of(new Rejection(reason, detail));
  }
}
