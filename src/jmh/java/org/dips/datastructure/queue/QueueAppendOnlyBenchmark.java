package org.dips.datastructure.queue;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(3)
public class QueueAppendOnlyBenchmark {

  @State(Scope.Benchmark)
  public static class LockFreeState {
    ConcurrentQueue<Integer> queue;

    @Setup(Level.Iteration)
    public void setup() {
      queue = new LockFreeQueue<>();
    }
  }

  @State(Scope.Benchmark)
  public static class LockedState {
    ConcurrentQueue<Integer> queue;

    @Setup(Level.Iteration)
    public void setup() {
      queue = new LockedQueue<>();
    }
  }

  @Benchmark
  public void lockFreeEnqueue(LockFreeState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  public void lockedEnqueue(LockedState state) {
    state.queue.enqueue(42);
  }
}