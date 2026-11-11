package com.pangate.contracts.testkit;

import java.nio.charset.StandardCharsets;

/**
 * State Push API writes and the gateway reads (push contracts spec §3.3, gateway spec §6.7): {@code seq:{principal}} on
 * Redis state ({@code INCR}; missing = 0) and hash {@code seqepochs}: slot → seqEpoch, slot = Redis Cluster slot of the
 * principal (CRC16-XMODEM mod 16384, hash tags honoured, as Lettuce {@code SlotHash.getSlot}).
 */
public final class SeqKeys {

  public static final String SEQ_EPOCHS = "seqepochs";
  public static final int SLOTS = 16384;

  private SeqKeys() {}

  public static String seq(String principal) {
    return "seq:{" + principal + "}";
  }

  /**
   * The {@code seqepochs} field of a principal: the slot of its {@code seq:{principal}} key, so the epoch is bumped
   * with the shard that holds the seq. Same as the principal's own slot unless the principal contains braces.
   */
  public static int epochSlot(String principal) {
    return slot(seq(principal));
  }

  public static int slot(String key) {
    byte[] b = key.getBytes(StandardCharsets.UTF_8);
    int start = 0;
    int end = b.length;
    for (int i = 0; i < b.length; i++) {
      if (b[i] == '{') {
        for (int j = i + 1; j < b.length; j++) {
          if (b[j] == '}') {
            if (j > i + 1) {
              start = i + 1;
              end = j;
            }
            break;
          }
        }
        break;
      }
    }
    return crc16(b, start, end) % SLOTS;
  }

  static int crc16(byte[] b, int from, int to) {
    int crc = 0;
    for (int i = from; i < to; i++) {
      crc ^= (b[i] & 0xff) << 8;
      for (int k = 0; k < 8; k++) {
        crc = (crc & 0x8000) != 0 ? (crc << 1) ^ 0x1021 : crc << 1;
      }
      crc &= 0xffff;
    }
    return crc;
  }
}
