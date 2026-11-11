package com.pangate.pushapi.core;

/** Cached {@code seqepochs} (slot → epoch); a slot that is absent is 0. */
public interface SeqEpochs {

  int epoch(String principal);

  /** Re-reads {@code seqepochs} now (after a bump or a seq regression). */
  void refreshNow();
}
