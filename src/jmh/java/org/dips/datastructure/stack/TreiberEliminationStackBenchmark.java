package org.dips.datastructure.stack;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 3, jvmArgsPrepend = "--enable-preview")
public class TreiberEliminationStackBenchmark {

  private TreiberEliminationStack<Integer> stack = new TreiberEliminationStack<>();

  @State(Scope.Thread)
  public static class ThreadState {
    int counter;
  }

  @Setup(Level.Trial)
  public void setUp() {
    stack = new TreiberEliminationStack<>();

    for (int i = 0; i < 100_000; i++) {
      stack.push(i);
    }
  }

  @Benchmark
  public void mixedPushPop(ThreadState state, Blackhole bh) {
    int c = state.counter++;

    if ((c & 1) == 0) {
      stack.push(c);
    } else {
      bh.consume(stack.pop());
    }
  }
}
