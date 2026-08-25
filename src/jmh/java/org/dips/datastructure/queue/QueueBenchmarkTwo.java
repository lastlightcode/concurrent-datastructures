package org.dips.datastructure.queue;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(3)
public class QueueBenchmarkTwo {

  @State(Scope.Group)
  public static class LockFreeState {

    ConcurrentQueue<Integer> queue;

    @Setup(Level.Iteration)
    public void setup() {
      queue = new LockFreeQueue<>();

      for (int i = 0; i < 1_000_000; i++) {
        queue.enqueue(i);
      }
    }
  }

  @State(Scope.Group)
  public static class LockedState {

    ConcurrentQueue<Integer> queue;

    @Setup(Level.Iteration)
    public void setup() {
      queue = new LockedQueue<>();

      for (int i = 0; i < 1_000_000; i++) {
        queue.enqueue(i);
      }
    }
  }

  /*
   * Michael-Scott
   *
   * 1 producer
   * 4 consumers
   */

  @Benchmark
  @Group("lockFree1P4C")
  @GroupThreads(1)
  public void lockFreeProducer(LockFreeState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("lockFree1P4C")
  @GroupThreads(4)
  public void lockFreeConsumer(
      LockFreeState state,
      Blackhole blackhole) {

    try {
      blackhole.consume(state.queue.dequeue());
    } catch (NoSuchElementException ignored) {
      // Queue temporarily empty.
    }
  }

  /*
   * ReentrantLock
   *
   * 1 producer
   * 4 consumers
   */

  @Benchmark
  @Group("locked1P4C")
  @GroupThreads(1)
  public void lockedProducer(LockedState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("locked1P4C")
  @GroupThreads(4)
  public void lockedConsumer(
      LockedState state,
      Blackhole blackhole) {

    try {
      blackhole.consume(state.queue.dequeue());
    } catch (NoSuchElementException ignored) {
      // Queue temporarily empty.
    }
  }
}
