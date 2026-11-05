package com.pangate.gateway.registry;

import java.util.OptionalLong;
import java.util.concurrent.CompletionStage;

/** Per-principal sequence for CONNECTED (spec §4.6 step 5, §6.7). */
public interface SeqReader {

  /** Current seq (missing key = 0); empty when unknown (timeout, error). Never completes exceptionally. */
  CompletionStage<OptionalLong> read(String principal);

  /** Cached seq epoch of the principal's slot; 0 until Push API bumped it. */
  int seqEpoch(String principal);
}
