package com.pangate.chat.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.pangate.chat.core.ChatStores.Privacy;
import com.pangate.contracts.v1.PresenceUpdate;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/** CH3 spec §6.2, §8: what a viewer is told about a user. */
class PresenceViewTest {

  static final long NOW = 1_800_000_000_000L;
  static final Privacy OPEN = Privacy.OPEN;

  @Test
  void roundsDownToMinute() {
    PresenceUpdate.State s = PresenceView.state("u", false, OptionalLong.of(NOW - 125_000), OPEN, OPEN, NOW);
    assertThat(s.getLastSeenMs()).isEqualTo((NOW - 125_000) / 60_000 * 60_000);
    assertThat(s.getOnline()).isFalse();
    assertThat(s.getRecently()).isFalse();
    assertThat(s.getUserId()).isEqualTo("u");
  }

  @Test
  void olderThan7DaysIsRecently() {
    PresenceUpdate.State s = PresenceView.state("u", false, OptionalLong.of(NOW - 8L * 86_400_000), OPEN, OPEN, NOW);
    assertThat(s.getRecently()).isTrue();
    assertThat(s.getLastSeenMs()).isZero();
  }

  @Test
  void hiddenWhenEitherSideHides() {
    Privacy hide = new Privacy(true, false);
    for (PresenceUpdate.State s : new PresenceUpdate.State[] {
        PresenceView.state("u", true, OptionalLong.of(NOW), hide, OPEN, NOW),
        PresenceView.state("u", true, OptionalLong.of(NOW), OPEN, hide, NOW)}) {
      assertThat(s.getHidden()).isTrue();
      assertThat(s.getOnline()).isFalse();
      assertThat(s.getLastSeenMs()).isZero();
    }
  }

  @Test
  void onlineHasNoLastSeen() {
    PresenceUpdate.State s = PresenceView.state("u", true, OptionalLong.of(NOW - 5_000), OPEN, OPEN, NOW);
    assertThat(s.getOnline()).isTrue();
    assertThat(s.getLastSeenMs()).isZero();
  }
}
