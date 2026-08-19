package org.dips.datastructure.stack;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsPrepend = "--enable-preview")
public class ConcurrentStackBenchmark {

  private ConcurrentStack<Integer> stack;

  private static final ThreadLocal<Integer> counter = ThreadLocal.withInitial(() -> 0);

  @Setup(Level.Trial)
  public void setUp() {
    stack = new ConcurrentStack<>();

    for (int i = 0; i < 10_000_000; i++) {
      stack.push(i);
    }
  }

  @Benchmark
  public void mixedPushPop(Blackhole bh) {
    int c = counter.get();
    counter.set(c + 1);

    if ((c & 1) == 0) {
      stack.push(c);
    } else {
      Integer val = stack.pop();
      if (val != null) {
        bh.consume(val);
      }
    }
  }
}