package com.pangate.chat.core;

import com.pangate.chat.core.ChatStores.Marks;
import com.pangate.chat.core.ChatStores.Member;
import com.pangate.chat.core.ChatStores.Privacy;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ConversationKind;
import com.pangate.contracts.v1.PushRequest;
import com.pangate.contracts.v1.ReceiptUpdate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * CH3 spec §3.4 step 2, §8: the pushes of one flushed window.
 * <ul>
 *   <li>Each reader whose read mark moved: {@code own_read_seq} to all their devices (unread sync).</li>
 *   <li>DM and groups up to {@code listedMax}: every other current member gets the marks that changed. For a pair
 *   that may not see reads (either side hides them) only a delivered mark from a {@code chat.delivered} command is
 *   sent, read as 0; a read alone sends nothing, so its timing tells nothing.</li>
 *   <li>Larger groups and channels: nobody else.</li>
 * </ul>
 * Users no longer members are neither listed nor told.
 */
public final class ReceiptRecipients {

  private ReceiptRecipients() {}

  /**
   * Perf 3a (spec §3.1): whom a flush is planned for. DM and group: the current members (cached list, at most 500). A
   * channel: only the readers of this flush, never its follower list. They were members when their marks were taken
   * ({@link MarkValidator}), and a channel flush tells nobody but each reader their own read mark, so one who left in
   * the meantime learns nothing new.
   */
  public static List<Member> audience(ReceiptAggregator.Flush f, MemberLookup members) {
    if (f.kind() != ConversationKind.CONVERSATION_KIND_CHANNEL) {
      return members.of(f.convId());
    }
    List<Member> out = new ArrayList<>(f.byUser().size());
    f.byUser().keySet().forEach(u -> out.add(new Member(u, com.pangate.chat.core.ChatStores.Role.MEMBER)));
    return out;
  }

  public static List<PushRequest> plan(ReceiptAggregator.Flush f, List<Member> members,
      Function<String, Privacy> privacy, int listedMax) {
    Set<String> current = members.stream().map(Member::userId).collect(Collectors.toSet());
    List<PushRequest> out = new ArrayList<>();
    for (Map.Entry<String, Marks> e : f.byUser().entrySet()) {
      if (e.getValue().readSeq() > 0 && current.contains(e.getKey())) {
        out.add(StatePushes.ephemeral(e.getKey(), "", ChatTopics.TYPE_RECEIPTS, ReceiptUpdate.newBuilder()
            .setConversationId(f.convId()).setOwnReadSeq(e.getValue().readSeq()).build().toByteString(),
            StatePushes.RECEIPTS_TTL_SEC));
      }
    }
    boolean listed = f.kind() == ConversationKind.CONVERSATION_KIND_DIRECT
        || (f.kind() == ConversationKind.CONVERSATION_KIND_GROUP && members.size() <= listedMax);
    if (!listed) {
      return out;
    }
    for (Member viewer : members) {
      Privacy pv = privacy.apply(viewer.userId());
      ReceiptUpdate.Builder u = ReceiptUpdate.newBuilder().setConversationId(f.convId());
      for (Map.Entry<String, Marks> e : f.byUser().entrySet()) {
        if (e.getKey().equals(viewer.userId()) || !current.contains(e.getKey())) {
          continue;
        }
        boolean showRead = PrivacyRules.mayShowRead(privacy.apply(e.getKey()), pv);
        if (showRead) {
          u.addMarks(ReceiptUpdate.Mark.newBuilder().setUserId(e.getKey())
              .setDeliveredSeq(e.getValue().deliveredSeq()).setReadSeq(e.getValue().readSeq()));
        } else {
          // final review C3: only a real chat.delivered moves what this pair sees; a read must leave no trace,
          // not even a push timed to it
          Long delivered = f.deliveredOnly().get(e.getKey());
          if (delivered != null && delivered > 0) {
            u.addMarks(ReceiptUpdate.Mark.newBuilder().setUserId(e.getKey()).setDeliveredSeq(delivered));
          }
        }
      }
      if (u.getMarksCount() > 0) {
        out.add(StatePushes.ephemeral(viewer.userId(), "", ChatTopics.TYPE_RECEIPTS, u.build().toByteString(),
            StatePushes.RECEIPTS_TTL_SEC));
      }
    }
    return out;
  }

  /**
   * Review 2026-11-19 (F3): whether {@code peer} sees anything of this flush, so their inbox row's state time may
   * move: a reader whose reads the pair may see, or a delivered mark from a real {@code chat.delivered}. A hidden read
   * alone stamps nothing, or {@code /v1/state?since} would time it.
   */
  public static boolean peerSeesChange(ReceiptAggregator.Flush f, String peer, Function<String, Privacy> privacy) {
    Privacy pv = privacy.apply(peer);
    for (Map.Entry<String, Marks> e : f.byUser().entrySet()) {
      if (e.getKey().equals(peer)) {
        continue;
      }
      if (PrivacyRules.mayShowRead(privacy.apply(e.getKey()), pv)) {
        return true;
      }
      Long delivered = f.deliveredOnly().get(e.getKey());
      if (delivered != null && delivered > 0) {
        return true;
      }
    }
    return false;
  }
}
