package com.pangate.router.consumer;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Parallel Consumer whose workers are virtual threads: a record waiting seconds for a gateway ACK parks a virtual
 * thread instead of holding a platform thread, so maxConcurrency can be tens of thousands (push-router spec §1, R4).
 * Relies on the protected {@code setupWorkerPool} of parallel-consumer 0.5.3.3; re-check on every upgrade.
 */
public final class VirtualThreadParallelConsumer<K, V> extends ParallelEoSStreamProcessor<K, V> {

  public VirtualThreadParallelConsumer(ParallelConsumerOptions<K, V> options) {
    super(options);
  }

  @Override
  protected ThreadPoolExecutor setupWorkerPool(int poolSize) {
    return new ThreadPoolExecutor(poolSize, poolSize, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
        Thread.ofVirtual().name("pc-worker-", 0).factory());
  }
}
