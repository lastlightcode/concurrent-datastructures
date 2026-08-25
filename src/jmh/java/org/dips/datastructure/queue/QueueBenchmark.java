package org.dips.datastructure.queue;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(3)
public class QueueBenchmark {

  @State(Scope.Benchmark)
  public static class LockFreeState {
    ConcurrentQueue<Integer> queue;

    @Setup(Level.Trial)
    public void setup() {
      queue = new LockFreeQueue<>();

      for (int i = 0; i < 10_000; i++) {
        queue.enqueue(i);
      }
    }
  }

  @State(Scope.Benchmark)
  public static class LockedState {
    ConcurrentQueue<Integer> queue;

    @Setup(Level.Trial)
    public void setup() {
      queue = new LockedQueue<>();

      for (int i = 0; i < 10_000; i++) {
        queue.enqueue(i);
      }
    }
  }

  @Benchmark
  public void lockFreeMixed(LockFreeState state, Blackhole blackhole) {
    state.queue.enqueue(42);
    blackhole.consume(state.queue.dequeue());
  }

  @Benchmark
  public void lockedMixed(LockedState state, Blackhole blackhole) {
    state.queue.enqueue(42);
    blackhole.consume(state.queue.dequeue());
  }
}