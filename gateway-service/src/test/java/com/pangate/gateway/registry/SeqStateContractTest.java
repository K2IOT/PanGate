package com.pangate.gateway.registry;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.contracts.testkit.SeqKeys;
import io.lettuce.core.cluster.SlotHash;
import org.junit.jupiter.api.Test;

/** The gateway reads seq:{p} and seqepochs exactly where Push API writes them (push contracts spec §3.3). */
class SeqStateContractTest {

  @Test
  void keysAndSlotsAgreeWithTheContract() {
    for (String p : new String[] {"t1:u1", "t2:someone@example", "tenant-x:42", "t1:é"}) {
      assertThat(RegistryKeys.seq(p)).isEqualTo(SeqKeys.seq(p));
      assertThat(SlotHash.getSlot(p)).as(p).isEqualTo(SeqKeys.slot(p));
    }
    assertThat(RegistryKeys.SEQ_EPOCHS).isEqualTo(SeqKeys.SEQ_EPOCHS);
  }
}
