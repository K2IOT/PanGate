package com.pangate.contracts.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** State Push API shares with the gateway (spec §3.3). */
class SeqKeysTest {

  @Test
  void keysAndRedisClusterSlots() {
    assertThat(SeqKeys.seq("t1:u1")).isEqualTo("seq:{t1:u1}");
    assertThat(SeqKeys.SEQ_EPOCHS).isEqualTo("seqepochs");
    // Redis Cluster CLUSTER KEYSLOT values
    assertThat(SeqKeys.slot("foo")).isEqualTo(12182);
    assertThat(SeqKeys.slot("bar")).isEqualTo(5061);
    assertThat(SeqKeys.slot("hello")).isEqualTo(866);
    assertThat(SeqKeys.slot("t1:u1")).isBetween(0, 16383);
  }

  /** PR review: the epoch of a principal lives at the slot of its seq key, the shard whose failover resets it. */
  @Test
  void epochSlotIsTheSlotOfTheSeqKey() {
    assertThat(SeqKeys.epochSlot("t1:{user}")).isEqualTo(SeqKeys.slot("seq:{t1:{user}}"))
        .isNotEqualTo(SeqKeys.slot("t1:{user}"));
    assertThat(SeqKeys.epochSlot("t1:u1")).isEqualTo(SeqKeys.slot("t1:u1"));   // no braces: unchanged
  }
}
