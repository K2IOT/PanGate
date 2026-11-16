package com.pangate.contracts.testkit;

/** Topic of the app chat's accounts (account spec §6). */
public final class AccountTopics {

  /** Compacted; key = userId, value = {@code AccountState}, no tombstones. */
  public static final String USERS = "account.users";
  public static final int PARTITIONS = 12;

  private AccountTopics() {}
}
