package org.dips.datastructure.queue;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 5, time = 3)
@Fork(3)
public class BasketsQueueBenchmark {

  @State(Scope.Benchmark)
  public static class MichaelScottState {

    LockFreeQueue<Integer> queue;

    @Setup(Level.Iteration)
    public void setup() {
      queue = new LockFreeQueue<>();
    }
  }

  @State(Scope.Benchmark)
  public static class BasketsState {

    BasketsQueue<Integer> queue;

    @Setup(Level.Iteration)
    public void setup() {
      queue = new BasketsQueue<>(16, 8);
    }
  }

  @State(Scope.Benchmark)
  public static class JdkState {

    ConcurrentLinkedQueue<Integer> queue;

    @Setup(Level.Iteration)
    public void setup() {
      queue = new ConcurrentLinkedQueue<>();
    }
  }

  @Benchmark
  public void michaelScott(MichaelScottState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  public void baskets(BasketsState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  public void concurrentLinkedQueue(JdkState state) {
    state.queue.offer(42);
  }
}
