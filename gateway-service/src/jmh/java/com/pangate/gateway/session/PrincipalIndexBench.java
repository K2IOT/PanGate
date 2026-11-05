package com.pangate.gateway.session;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/** Spec §11: PrincipalIndex lookups and add/remove with 100k principals (spec §3.4). */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class PrincipalIndexBench {

  @Param({"100000"})
  public int principals;

  private PrincipalIndex index;
  private String[] names;

  @Setup(Level.Trial)
  public void setUp() {
    index = new PrincipalIndex();
    names = new String[principals];
    for (int i = 0; i < principals; i++) {
      names[i] = "t1:u" + i;
      index.add(names[i], new ConnRef(i & 15, i));
    }
  }

  @Benchmark
  public List<ConnRef> get() {
    return index.get(names[ThreadLocalRandom.current().nextInt(principals)]);
  }

  /** A second device of an existing principal connects and leaves (array value path). */
  @Benchmark
  public int addRemove() {
    String p = names[ThreadLocalRandom.current().nextInt(principals)];
    ConnRef ref = new ConnRef(3, -1L);
    index.add(p, ref);
    index.remove(p, ref);
    return index.principalCount();
  }
}
