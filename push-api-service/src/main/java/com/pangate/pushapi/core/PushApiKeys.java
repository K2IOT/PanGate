package com.pangate.pushapi.core;

/** Redis state keys of Push API (push-api spec §2.1, §3, §4.2; push contracts §3.3). */
public final class PushApiKeys {

  /** Same slot as {@code seqepochs} (hash tag), so the bump script sees both keys. */
  public static final String RUNIDS = "runids{seqepochs}";

  private PushApiKeys() {}

  public static String topicSeq(String topic) {
    return "topicseq:{" + topic + "}";
  }

  public static String idem(String tenant, String key) {
    return "idem:{" + tenant + "}:" + key;
  }

  /** Own prefix, so a client key ending in {@code :last} cannot land on another key's copy. */
  public static String idemLast(String tenant, String key) {
    return "idemlast:{" + tenant + "}:" + key;
  }

  public static String msg(String tenant, String msgId) {
    return "msg:{" + tenant + "}:" + msgId;
  }
}
