package com.pangate.devtools.cmd;

import com.pangate.contracts.v1.OfflineRecord;
import com.pangate.devtools.push.OfflineAwaiter;
import java.time.Duration;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "await-offline", description = "Wait for the OfflineRecord of a msg_id on cN.push.offline; print reason "
    + "and attempts. Exit 1 on timeout or, with --reason, on another reason.")
public final class AwaitOfflineCommand implements Callable<Integer> {

  @Option(names = "--kafka", required = true)
  String kafka;

  @Option(names = "--cell", defaultValue = "c1")
  String cell;

  @Option(names = "--msg-id", required = true)
  String msgId;

  @Option(names = "--reason", description = "expected reason, e.g. NO_SESSION")
  String reason;

  @Option(names = "--timeout", defaultValue = "PT30S")
  Duration timeout;

  @Override
  public Integer call() {
    try (OfflineAwaiter a = new OfflineAwaiter(kafka, cell, true)) {
      OfflineRecord o = a.await(msgId, timeout);
      System.out.println(o.getReason() + " attempts=" + o.getAttempts());
      return reason == null || o.getReason().name().equals("REASON_" + reason.toUpperCase(java.util.Locale.ROOT))
          ? 0 : 1;
    } catch (IllegalStateException e) {
      System.out.println(e.getMessage());
      return 1;
    }
  }
}
