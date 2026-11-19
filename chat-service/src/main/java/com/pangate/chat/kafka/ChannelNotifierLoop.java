package com.pangate.chat.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pangate.chat.core.ChannelNotifyPlan;
import com.pangate.chat.core.ChatStores;
import com.pangate.chat.core.NotificationComposer;
import com.pangate.chat.core.NotifySink;
import com.pangate.chat.core.PresenceState;
import com.pangate.chat.core.PrivacyCache;
import com.pangate.chat.core.SenderNames;
import com.pangate.contracts.testkit.ChatTopics;
import com.pangate.contracts.v1.ChatEvent;
import com.pangate.contracts.v1.ChatMessage;
import com.pangate.contracts.v1.ConversationKind;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CH4 spec §4: group {@code chat-channel-notify} on {@code chat.events}; a channel post becomes a job that pages the
 * channel's members, a page at a time, round robin over the live jobs, and produces one NotifyRequest per offline,
 * unmuted follower (badge unchanged, collapse id = the channel).
 *
 * <ul>
 *   <li>One live job per channel: a newer post takes over the running job's cursor, so a follower gets one
 *       notification per burst, and it shows the newest post they were reached with.</li>
 *   <li>After each page the cursor is saved ({@code channel_notify_progress}); a job read again after a restart or a
 *       rebalance goes on from it. Only the last page can be sent twice, and notify-service drops the repeats by
 *       request id.</li>
 *   <li>At most {@code ratePerSec} requests per second for this pod; a post older than {@code maxAge} is dropped.</li>
 *   <li>A partition's offset is committed up to its oldest unfinished job.</li>
 * </ul>
 */
public final class ChannelNotifierLoop implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(ChannelNotifierLoop.class);
  public static final String GROUP = "chat-channel-notify";
  static final long MAX_BACKOFF_MS = 5_000;

  private static final class Job {
    ChatMessage post;
    String title;
    String after;
    final TopicPartition tp;
    final long offset;

    Job(ChatMessage post, String title, String after, TopicPartition tp, long offset) {
      this.post = post;
      this.title = title;
      this.after = after;
      this.tp = tp;
      this.offset = offset;
    }
  }

  private final Consumer<String, byte[]> consumer;
  private final String topic;
  private final ChatStores.MuteStore members;
  private final ChatStores.MemberStore membership;
  private final PresenceState presence;
  private final ChatStores.ChannelProgressStore progress;
  private final ChatStores.ConversationStore conversations;
  private final PrivacyCache privacy;
  private final SenderNames names;
  private final NotifySink sink;
  private final int pageSize;
  private final long nanosPerRequest;
  private final long maxAgeMs;
  private final MeterRegistry meters;
  private final LongSupplier wallMs;
  private final Map<String, Job> live = new LinkedHashMap<>();
  private final Map<TopicPartition, Long> next = new HashMap<>();
  private final Map<TopicPartition, Long> committed = new HashMap<>();
  private long paceFrom = System.nanoTime();
  private volatile boolean running = true;
  private Thread thread;

  public <S extends ChatStores.MuteStore & ChatStores.MemberStore> ChannelNotifierLoop(Consumer<String, byte[]> consumer,
      String topic, S members,
      PresenceState presence, ChatStores.ChannelProgressStore progress, ChatStores.ConversationStore conversations,
      PrivacyCache privacy, ChatStores.UserStore users, NotifySink sink, int pageSize, int ratePerSec,
      Duration maxAge, MeterRegistry meters, LongSupplier wallMs) {
    this.consumer = consumer;
    this.topic = topic;
    this.members = members;
    this.membership = members;
    this.presence = presence;
    this.progress = progress;
    this.conversations = conversations;
    this.privacy = privacy;
    this.names = new SenderNames(users);
    this.sink = sink;
    this.pageSize = pageSize;
    this.nanosPerRequest = 1_000_000_000L / Math.max(1, ratePerSec);
    this.maxAgeMs = maxAge.toMillis();
    this.meters = meters;
    this.wallMs = wallMs;
  }

  public static KafkaConsumer<String, byte[]> consumer(String bootstrap, String group) {
    return new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
        ConsumerConfig.GROUP_ID_CONFIG, group,
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
        // earliest: a new group must not skip posts written while it was joining; older than maxAge are dropped
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
        ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500,
        // a page of a big channel takes a while at the paced rate; polls still happen between pages
        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 600_000), new StringDeserializer(), new ByteArrayDeserializer());
  }

  public synchronized void start() {
    thread = Thread.ofPlatform().daemon().name("chat-channel-notify").start(this::run);
  }

  private void run() {
    long backoff = 100;
    try {
      consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> parts) {
          live.values().removeIf(j -> parts.contains(j.tp));   // their cursors are saved; the next owner goes on
          parts.forEach(next::remove);
          parts.forEach(committed::remove);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> parts) {}
      });
      while (running) {
        try {
          // final review: a record whose store reads fail is read again, never committed past
          java.util.Set<TopicPartition> rewound = new java.util.HashSet<>();
          RuntimeException failed = null;
          for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofMillis(live.isEmpty() ? 500 : 0))) {
            TopicPartition tp = new TopicPartition(r.topic(), r.partition());
            if (rewound.contains(tp)) {
              continue;
            }
            try {
              take(r);
            } catch (RuntimeException e) {
              next.put(tp, r.offset());
              consumer.seek(tp, r.offset());
              rewound.add(tp);
              failed = e;
            }
          }
          if (failed != null) {
            throw failed;
          }
          for (Job j : List.copyOf(live.values())) {
            step(j);
          }
          commit();
          backoff = 100;
        } catch (WakeupException e) {
          throw e;
        } catch (RuntimeException e) {
          meters.counter("chat.notify.channel.retries").increment();
          log.warn("channel notifications: {}; trying again", e.toString());
          Thread.sleep(backoff);
          backoff = Math.min(MAX_BACKOFF_MS, backoff * 2);
        }
      }
    } catch (WakeupException e) {
      // closing
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      consumer.close(Duration.ofSeconds(2));
    }
  }

  private void take(ConsumerRecord<String, byte[]> r) {
    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
    next.put(tp, r.offset() + 1);
    ChatEvent e;
    try {
      e = ChatEvent.parseFrom(r.value() == null ? new byte[0] : r.value());
    } catch (InvalidProtocolBufferException ex) {
      return;
    }
    if (e.getKind() != ConversationKind.CONVERSATION_KIND_CHANNEL) {
      return;
    }
    ChatMessage post = e.getMessage();
    String conv = post.getConversationId();
    if (wallMs.getAsLong() - post.getSentAtMs() > maxAgeMs) {
      // CH4 deferred minor: a post read again after a long outage leaves no unfinished cursor behind for a
      // later post to take over (its followers before the cursor would never hear of the later one)
      Optional<ChatStores.ChannelProgress> saved = progress.loadChannelProgress(conv);
      if (saved.isPresent() && saved.get().convSeq() == post.getConvSeq() && !saved.get().done()) {
        progress.saveChannelProgress(conv, post.getConvSeq(), ChatStores.ChannelProgress.DONE);
      }
      meters.counter("chat.notify.channel.jobs", "result", "stale").increment();
      return;
    }
    Job running = live.get(conv);
    if (running != null) {
      if (running.post.getConvSeq() < post.getConvSeq()) {
        notifyPassedMentions(post, running.title, running.after);
        running.post = post;   // takes over the cursor (class doc)
        progress.saveChannelProgress(conv, post.getConvSeq(), running.after);
        meters.counter("chat.notify.channel.jobs", "result", "replaced").increment();
      }
      return;
    }
    String after = "";
    Optional<ChatStores.ChannelProgress> saved = progress.loadChannelProgress(conv);
    if (saved.isPresent()) {
      ChatStores.ChannelProgress p = saved.get();
      if (p.convSeq() > post.getConvSeq() || (p.convSeq() == post.getConvSeq() && p.done())) {
        return;   // read again after a restart: a newer post has it, or this one is done
      }
      if (!p.done()) {
        after = p.lastUserId();   // this post's cursor, or an unfinished older post's cursor to take over
      }
    }
    Optional<ChatStores.Conversation> c = conversations.get(conv);
    if (c.isEmpty()) {
      return;
    }
    if (!after.isEmpty() && saved.get().convSeq() < post.getConvSeq()) {
      notifyPassedMentions(post, c.get().title(), after);   // took over an older post's cursor
    }
    live.put(conv, new Job(post, c.get().title(), after, tp, r.offset()));
    meters.counter("chat.notify.channel.jobs", "result", "started").increment();
  }

  /** One page of the job: notify its recipients, save the cursor; an empty page ends the job. */
  private void step(Job j) throws InterruptedException {
    String conv = j.post.getConversationId();
    long now = wallMs.getAsLong();
    if (now - j.post.getSentAtMs() > maxAgeMs) {
      finish(j, "stale");
      return;
    }
    List<ChatStores.MemberMute> page = members.membersPage(conv, j.after, pageSize);
    if (page.isEmpty()) {
      finish(j, "done");
      return;
    }
    List<String> ids = new ArrayList<>(page.size());
    page.forEach(m -> ids.add(m.userId()));
    List<String> to = ChannelNotifyPlan.recipients(page, received(conv, ids), j.post, now);
    String sender = names.of(j.post.getSenderId(), now);
    List<CompletableFuture<Void>> sent = new ArrayList<>(to.size());
    for (String user : to) {
      pace();
      sent.add(sink.send(NotificationComposer.compose(j.post, ConversationKind.CONVERSATION_KIND_CHANNEL, j.title,
          sender, privacy.of(user).hidePreview(), -1, user, ChatTopics.TENANT)));
    }
    CompletableFuture.allOf(sent.toArray(CompletableFuture[]::new)).join();
    meters.counter("chat.notify.channel.sent").increment(to.size());
    j.after = page.getLast().userId();
    progress.saveChannelProgress(conv, j.post.getConvSeq(), j.after);
  }

  /**
   * CH4 deferred minor: a post that takes over a cursor reaches only the followers after it; those it mentions
   * before it (members, not the sender, offline) get it now, mentions breaking through mute (spec §3.3).
   */
  private void notifyPassedMentions(ChatMessage post, String title, String cursor) {
    if (cursor.isEmpty() || cursor.equals(ChatStores.ChannelProgress.DONE)) {
      return;
    }
    String conv = post.getConversationId();
    List<String> passed = new ArrayList<>();
    for (String u : new java.util.LinkedHashSet<>(post.getMentionUserIdsList())) {
      if (u.compareTo(cursor) <= 0 && !u.equals(post.getSenderId()) && membership.member(conv, u).isPresent()) {
        passed.add(u);
      }
    }
    if (passed.isEmpty()) {
      return;
    }
    java.util.Set<String> online = received(conv, passed);
    long now = wallMs.getAsLong();
    String sender = names.of(post.getSenderId(), now);
    List<CompletableFuture<Void>> sent = new ArrayList<>();
    for (String user : passed) {
      if (!online.contains(user)) {
        sent.add(sink.send(NotificationComposer.compose(post, ConversationKind.CONVERSATION_KIND_CHANNEL, title,
            sender, privacy.of(user).hidePreview(), -1, user, ChatTopics.TENANT)));
      }
    }
    CompletableFuture.allOf(sent.toArray(CompletableFuture[]::new)).join();
    meters.counter("chat.notify.channel.sent").increment(sent.size());
  }

  private void finish(Job j, String result) {
    progress.saveChannelProgress(j.post.getConversationId(), j.post.getConvSeq(), ChatStores.ChannelProgress.DONE);
    live.remove(j.post.getConversationId());
    meters.counter("chat.notify.channel.jobs", "result", result).increment();
  }

  /** Keeps this pod at {@code ratePerSec}; a pause longer than a second does not bank credit. */
  private void pace() throws InterruptedException {
    long now = System.nanoTime();
    if (now - paceFrom > 1_000_000_000L) {
      paceFrom = now;
    }
    paceFrom += nanosPerRequest;
    long wait = paceFrom - now;
    if (wait > 0) {
      Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
    }
  }

  /** Up to each partition's oldest unfinished job, or everything taken when none is left. */
  private void commit() {
    Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> n : next.entrySet()) {
      long safe = n.getValue();
      for (Job j : live.values()) {
        if (j.tp.equals(n.getKey())) {
          safe = Math.min(safe, j.offset);
        }
      }
      if (!Long.valueOf(safe).equals(committed.get(n.getKey()))) {
        out.put(n.getKey(), new OffsetAndMetadata(safe));
      }
    }
    if (!out.isEmpty()) {
      consumer.commitSync(out);
      out.forEach((tp, o) -> committed.put(tp, o.offset()));
    }
  }

  @Override
  public synchronized void close() {
    running = false;
    consumer.wakeup();
    if (thread != null) {
      try {
        thread.join(10_000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Review 2026-11-19 (F6): who got the post live: online and with the channel open. An online follower without it
   * open gets the notification like an offline one (CH4 §4.2 treated any online follower as having received it).
   */
  private java.util.Set<String> received(String conv, List<String> ids) {
    java.util.Set<String> online = presence.onlineOf(ids);
    return online.isEmpty() ? online : presence.haveOpen(conv, online);
  }
}
