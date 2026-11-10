package com.pangate.pushapi.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.testkit.SeqKeys;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class SeqAllocatorTest {

  final List<String> keys = new ArrayList<>();
  final Deque<Long> answers = new ArrayDeque<>();
  final SeqStore store = key -> {
    keys.add(key);
    return answers.removeFirst();
  };
  final AtomicInteger refreshes = new AtomicInteger();
  int epoch = 3;
  final SeqEpochs epochs = new SeqEpochs() {
    @Override public int epoch(String principal) {
      return SeqKeys.slot(principal) == SeqKeys.slot("t1:u1") ? epoch : 0;
    }
    @Override public void refreshNow() { refreshes.incrementAndGet(); epoch = 4; }
  };
  final AtomicInteger regressions = new AtomicInteger();
  final SeqAllocator alloc = new SeqAllocator(store, epochs, regressions::incrementAndGet);

  @Test
  void principalSeqIsIncrWithTheSlotEpoch() {
    answers.add(42L);
    assertThat(alloc.forPrincipal("t1:u1")).isEqualTo(new Seq(42, 3));
    assertThat(keys).containsExactly("seq:{t1:u1}");
  }

  @Test
  void topicSeqUsesTopicseqKey() {
    answers.add(7L);
    assertThat(alloc.forTopic("t1:news")).isEqualTo(7);
    assertThat(keys).containsExactly("topicseq:{t1:news}");
  }

  @Test
  void regressionRefreshesEpochAndReallocates() {
    answers.addAll(List.of(10L, 5L, 6L));
    assertThat(alloc.forPrincipal("t1:u1")).isEqualTo(new Seq(10, 3));
    assertThat(alloc.forPrincipal("t1:u1")).isEqualTo(new Seq(6, 4));
    assertThat(regressions).hasValue(1);
    assertThat(refreshes).hasValue(1);
    assertThat(keys).hasSize(3);
  }

  @Test
  void keysHelpers() {
    assertThat(PushApiKeys.idem("t1", "k")).isEqualTo("idem:{t1}:k");
    assertThat(PushApiKeys.idemLast("t1", "k")).isEqualTo("idemlast:{t1}:k");
    assertThat(PushApiKeys.msg("t1", "M")).isEqualTo("msg:{t1}:M");
    assertThat(PushApiKeys.RUNIDS).isEqualTo("runids{seqepochs}");
    assertThat(SeqKeys.slot(PushApiKeys.RUNIDS)).isEqualTo(SeqKeys.slot(SeqKeys.SEQ_EPOCHS));
  }
}
