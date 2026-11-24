package com.pangate.chat.core;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/** Records badge updates as text ({@code raise u c last read}, {@code raiseAll [..] c last}, {@code drop u c}). */
public final class RecordingBadges implements BadgeUpdates {

  public final List<String> calls = new CopyOnWriteArrayList<>();
  /** Runs on each call, before it is recorded: what the stores hold at that moment. */
  public volatile Consumer<String> onCall = s -> { };

  private void add(String s) {
    onCall.accept(s);
    calls.add(s);
  }

  @Override
  public void raise(String user, String convId, long last, long read) {
    add("raise " + user + " " + convId + " " + last + " " + read);
  }

  @Override
  public void raiseAll(List<String> users, String convId, long last) {
    add("raiseAll " + users.stream().sorted().toList() + " " + convId + " " + last);
  }

  @Override
  public void join(String user, String convId, long last) {
    add("join " + user + " " + convId + " " + last);
  }

  @Override
  public void drop(String user, String convId) {
    add("drop " + user + " " + convId);
  }
}
