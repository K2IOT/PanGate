package com.pangate.gateway.test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.LoggerFactory;

/**
 * Counts Netty {@code LEAK:} reports (spec §11: fuzz runs must not leak {@code ByteBuf}). Reads the detector's log,
 * so it works whatever order Netty classes were initialised in. Reports carry the allocation stack, so
 * {@link #leaks(String)} counts only leaks created by one test class: a leak from another class in the same fork is
 * reported whenever the GC finds it.
 */
public final class LeakWatch {

  private static final AtomicInteger LEAKS = new AtomicInteger();
  private static final Queue<String> REPORTS = new ConcurrentLinkedQueue<>();
  private static volatile boolean installed;

  private LeakWatch() {}

  public static synchronized void install() {
    if (installed) {
      return;
    }
    Logger logger = (Logger) LoggerFactory.getLogger(ResourceLeakDetector.class);
    AppenderBase<ILoggingEvent> appender = new AppenderBase<>() {
      @Override
      protected void append(ILoggingEvent e) {
        if (e.getLevel().isGreaterOrEqual(Level.ERROR) && e.getFormattedMessage().contains("LEAK:")) {
          LEAKS.incrementAndGet();
          REPORTS.add(e.getFormattedMessage());
        }
      }
    };
    appender.setName("leak-watch");
    appender.setContext(logger.getLoggerContext());
    appender.start();
    logger.addAppender(appender);
    installed = true;
  }

  public static int leaks() {
    return LEAKS.get();
  }

  /** Leaks whose report (allocation stack included) mentions {@code marker}, e.g. a test class name. */
  public static int leaks(String marker) {
    return (int) REPORTS.stream().filter(r -> r.contains(marker)).count();
  }

  public static void reset() {
    LEAKS.set(0);
    REPORTS.clear();
  }

  /** Lets the detector see collected buffers: it reports on the next tracked allocation after a GC. */
  public static void collect() {
    for (int round = 0; round < 5; round++) {
      System.gc();
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      for (int i = 0; i < 1000; i++) {
        ByteBuf b = PooledByteBufAllocator.DEFAULT.buffer(64);
        b.release();
      }
    }
  }
}
