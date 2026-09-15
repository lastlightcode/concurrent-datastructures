package org.dips.datastructure.queue;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 3)
@Fork(3)
public class BasketsQueueWorkloadBenchmark {

  private static final int MAX_RETRY_ATTEMPTS = 16;
  private static final int MAX_JUMPS = 8;

  /*
   * Counts useful consumer outcomes separately from raw benchmark invocations.
   * Every consumer benchmark invocation performs exactly ONE dequeue/poll.
   * This is deliberately bounded so a JMH iteration can always terminate.
   */
  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  public static class ConsumerCounters {

    public long successfulDequeues;
    public long emptyDequeues;

    @Setup(Level.Iteration)
    public void reset() {
      successfulDequeues = 0;
      emptyDequeues = 0;
    }
  }

  /*
   * -------------------------------------------------------------
   * STATES FOR @GROUP BENCHMARKS
   * -------------------------------------------------------------
   *
   * Scope.Group means every JMH group gets its own shared queue.
   *
   * So:
   *
   * basketsBalanced1P1C
   * basketsBalanced2P2C
   * basketsProducerHeavy8P1C
   *
   * etc. do NOT share queues with one another.
   */

  @State(Scope.Group)
  public static class BasketsGroupState {

    BasketsQueue<Integer> queue;

    @Setup(Level.Iteration)
    public void setup() {
      queue = new BasketsQueue<>(
          MAX_RETRY_ATTEMPTS,
          MAX_JUMPS
      );
    }
  }

  @State(Scope.Group)
  public static class MichaelScottGroupState {

    LockFreeQueue<Integer> queue;

    @Setup(Level.Iteration)
    public void setup() {
      queue = new LockFreeQueue<>();
    }
  }

  @State(Scope.Group)
  public static class ClqGroupState {

    ConcurrentLinkedQueue<Integer> queue;

    @Setup(Level.Iteration)
    public void setup() {
      queue = new ConcurrentLinkedQueue<>();
    }
  }

  /*
   * -------------------------------------------------------------
   * BALANCED
   * -------------------------------------------------------------
   *
   * 1P / 1C
   * 2P / 2C
   * 4P / 4C
   * 8P / 8C
   */

  // =============================================================
  // BASKETS - BALANCED
  // =============================================================

  @Benchmark
  @Group("basketsBalanced1P1C")
  @GroupThreads(1)
  public void basketsBalanced1P1CProducer(BasketsGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("basketsBalanced1P1C")
  @GroupThreads(1)
  public Integer basketsBalanced1P1CConsumer(BasketsGroupState state,
                                             ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  @Benchmark
  @Group("basketsBalanced2P2C")
  @GroupThreads(2)
  public void basketsBalanced2P2CProducer(BasketsGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("basketsBalanced2P2C")
  @GroupThreads(2)
  public Integer basketsBalanced2P2CConsumer(BasketsGroupState state,
                                             ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  @Benchmark
  @Group("basketsBalanced4P4C")
  @GroupThreads(4)
  public void basketsBalanced4P4CProducer(BasketsGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("basketsBalanced4P4C")
  @GroupThreads(4)
  public Integer basketsBalanced4P4CConsumer(BasketsGroupState state,
                                             ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  @Benchmark
  @Group("basketsBalanced8P8C")
  @GroupThreads(8)
  public void basketsBalanced8P8CProducer(BasketsGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("basketsBalanced8P8C")
  @GroupThreads(8)
  public Integer basketsBalanced8P8CConsumer(BasketsGroupState state,
                                             ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  // =============================================================
  // MICHAEL-SCOTT - BALANCED
  // =============================================================

  @Benchmark
  @Group("msBalanced1P1C")
  @GroupThreads(1)
  public void msBalanced1P1CProducer(MichaelScottGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("msBalanced1P1C")
  @GroupThreads(1)
  public Integer msBalanced1P1CConsumer(MichaelScottGroupState state,
                                        ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  @Benchmark
  @Group("msBalanced2P2C")
  @GroupThreads(2)
  public void msBalanced2P2CProducer(MichaelScottGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("msBalanced2P2C")
  @GroupThreads(2)
  public Integer msBalanced2P2CConsumer(MichaelScottGroupState state,
                                        ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  @Benchmark
  @Group("msBalanced4P4C")
  @GroupThreads(4)
  public void msBalanced4P4CProducer(MichaelScottGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("msBalanced4P4C")
  @GroupThreads(4)
  public Integer msBalanced4P4CConsumer(MichaelScottGroupState state,
                                        ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  @Benchmark
  @Group("msBalanced8P8C")
  @GroupThreads(8)
  public void msBalanced8P8CProducer(MichaelScottGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("msBalanced8P8C")
  @GroupThreads(8)
  public Integer msBalanced8P8CConsumer(MichaelScottGroupState state,
                                        ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  // =============================================================
  // JDK CONCURRENTLINKEDQUEUE - BALANCED
  // =============================================================

  @Benchmark
  @Group("clqBalanced1P1C")
  @GroupThreads(1)
  public void clqBalanced1P1CProducer(ClqGroupState state) {
    state.queue.offer(42);
  }

  @Benchmark
  @Group("clqBalanced1P1C")
  @GroupThreads(1)
  public Integer clqBalanced1P1CConsumer(ClqGroupState state,
                                         ConsumerCounters counters) {
    return pollOnce(state.queue, counters);
  }


  @Benchmark
  @Group("clqBalanced2P2C")
  @GroupThreads(2)
  public void clqBalanced2P2CProducer(ClqGroupState state) {
    state.queue.offer(42);
  }

  @Benchmark
  @Group("clqBalanced2P2C")
  @GroupThreads(2)
  public Integer clqBalanced2P2CConsumer(ClqGroupState state,
                                         ConsumerCounters counters) {
    return pollOnce(state.queue, counters);
  }


  @Benchmark
  @Group("clqBalanced4P4C")
  @GroupThreads(4)
  public void clqBalanced4P4CProducer(ClqGroupState state) {
    state.queue.offer(42);
  }

  @Benchmark
  @Group("clqBalanced4P4C")
  @GroupThreads(4)
  public Integer clqBalanced4P4CConsumer(ClqGroupState state,
                                         ConsumerCounters counters) {
    return pollOnce(state.queue, counters);
  }


  @Benchmark
  @Group("clqBalanced8P8C")
  @GroupThreads(8)
  public void clqBalanced8P8CProducer(ClqGroupState state) {
    state.queue.offer(42);
  }

  @Benchmark
  @Group("clqBalanced8P8C")
  @GroupThreads(8)
  public Integer clqBalanced8P8CConsumer(ClqGroupState state,
                                         ConsumerCounters counters) {
    return pollOnce(state.queue, counters);
  }


  /*
   * -------------------------------------------------------------
   * PRODUCER HEAVY
   * -------------------------------------------------------------
   *
   * 4P / 1C
   * 8P / 1C
   * 16P / 1C
   */

  // =============================================================
  // BASKETS - PRODUCER HEAVY
  // =============================================================

  @Benchmark
  @Group("basketsProducerHeavy4P1C")
  @GroupThreads(4)
  public void basketsProducerHeavy4P1CProducer(BasketsGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("basketsProducerHeavy4P1C")
  @GroupThreads(1)
  public Integer basketsProducerHeavy4P1CConsumer(BasketsGroupState state,
                                                  ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  @Benchmark
  @Group("basketsProducerHeavy8P1C")
  @GroupThreads(8)
  public void basketsProducerHeavy8P1CProducer(BasketsGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("basketsProducerHeavy8P1C")
  @GroupThreads(1)
  public Integer basketsProducerHeavy8P1CConsumer(BasketsGroupState state,
                                                  ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  @Benchmark
  @Group("basketsProducerHeavy16P1C")
  @GroupThreads(16)
  public void basketsProducerHeavy16P1CProducer(BasketsGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("basketsProducerHeavy16P1C")
  @GroupThreads(1)
  public Integer basketsProducerHeavy16P1CConsumer(BasketsGroupState state,
                                                   ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  // =============================================================
  // MICHAEL-SCOTT - PRODUCER HEAVY
  // =============================================================

  @Benchmark
  @Group("msProducerHeavy4P1C")
  @GroupThreads(4)
  public void msProducerHeavy4P1CProducer(MichaelScottGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("msProducerHeavy4P1C")
  @GroupThreads(1)
  public Integer msProducerHeavy4P1CConsumer(MichaelScottGroupState state,
                                             ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  @Benchmark
  @Group("msProducerHeavy8P1C")
  @GroupThreads(8)
  public void msProducerHeavy8P1CProducer(MichaelScottGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("msProducerHeavy8P1C")
  @GroupThreads(1)
  public Integer msProducerHeavy8P1CConsumer(MichaelScottGroupState state,
                                             ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  @Benchmark
  @Group("msProducerHeavy16P1C")
  @GroupThreads(16)
  public void msProducerHeavy16P1CProducer(MichaelScottGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("msProducerHeavy16P1C")
  @GroupThreads(1)
  public Integer msProducerHeavy16P1CConsumer(MichaelScottGroupState state,
                                              ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  // =============================================================
  // JDK CLQ - PRODUCER HEAVY
  // =============================================================

  @Benchmark
  @Group("clqProducerHeavy4P1C")
  @GroupThreads(4)
  public void clqProducerHeavy4P1CProducer(ClqGroupState state) {
    state.queue.offer(42);
  }

  @Benchmark
  @Group("clqProducerHeavy4P1C")
  @GroupThreads(1)
  public Integer clqProducerHeavy4P1CConsumer(ClqGroupState state,
                                              ConsumerCounters counters) {
    return pollOnce(state.queue, counters);
  }


  @Benchmark
  @Group("clqProducerHeavy8P1C")
  @GroupThreads(8)
  public void clqProducerHeavy8P1CProducer(ClqGroupState state) {
    state.queue.offer(42);
  }

  @Benchmark
  @Group("clqProducerHeavy8P1C")
  @GroupThreads(1)
  public Integer clqProducerHeavy8P1CConsumer(ClqGroupState state,
                                              ConsumerCounters counters) {
    return pollOnce(state.queue, counters);
  }


  @Benchmark
  @Group("clqProducerHeavy16P1C")
  @GroupThreads(16)
  public void clqProducerHeavy16P1CProducer(ClqGroupState state) {
    state.queue.offer(42);
  }

  @Benchmark
  @Group("clqProducerHeavy16P1C")
  @GroupThreads(1)
  public Integer clqProducerHeavy16P1CConsumer(ClqGroupState state,
                                               ConsumerCounters counters) {
    return pollOnce(state.queue, counters);
  }


  /*
   * -------------------------------------------------------------
   * CONSUMER HEAVY
   * -------------------------------------------------------------
   *
   * 1P / 4C
   * 1P / 8C
   * 1P / 16C
   *
   * Every consumer invocation performs exactly one dequeue/poll.
   *
   * Empty results are counted separately with ConsumerCounters instead of
   * spinning until success. This avoids trapping JMH workers at iteration
   * shutdown while still letting us distinguish useful dequeues from misses.
   */

  // =============================================================
  // BASKETS - CONSUMER HEAVY
  // =============================================================

  @Benchmark
  @Group("basketsConsumerHeavy1P4C")
  @GroupThreads(1)
  public void basketsConsumerHeavy1P4CProducer(BasketsGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("basketsConsumerHeavy1P4C")
  @GroupThreads(4)
  public Integer basketsConsumerHeavy1P4CConsumer(BasketsGroupState state,
                                                  ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  @Benchmark
  @Group("basketsConsumerHeavy1P8C")
  @GroupThreads(1)
  public void basketsConsumerHeavy1P8CProducer(BasketsGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("basketsConsumerHeavy1P8C")
  @GroupThreads(8)
  public Integer basketsConsumerHeavy1P8CConsumer(BasketsGroupState state,
                                                  ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  @Benchmark
  @Group("basketsConsumerHeavy1P16C")
  @GroupThreads(1)
  public void basketsConsumerHeavy1P16CProducer(BasketsGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("basketsConsumerHeavy1P16C")
  @GroupThreads(16)
  public Integer basketsConsumerHeavy1P16CConsumer(BasketsGroupState state,
                                                   ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  // =============================================================
  // MICHAEL-SCOTT - CONSUMER HEAVY
  // =============================================================

  @Benchmark
  @Group("msConsumerHeavy1P4C")
  @GroupThreads(1)
  public void msConsumerHeavy1P4CProducer(MichaelScottGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("msConsumerHeavy1P4C")
  @GroupThreads(4)
  public Integer msConsumerHeavy1P4CConsumer(MichaelScottGroupState state,
                                             ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  @Benchmark
  @Group("msConsumerHeavy1P8C")
  @GroupThreads(1)
  public void msConsumerHeavy1P8CProducer(MichaelScottGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("msConsumerHeavy1P8C")
  @GroupThreads(8)
  public Integer msConsumerHeavy1P8CConsumer(MichaelScottGroupState state,
                                             ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  @Benchmark
  @Group("msConsumerHeavy1P16C")
  @GroupThreads(1)
  public void msConsumerHeavy1P16CProducer(MichaelScottGroupState state) {
    state.queue.enqueue(42);
  }

  @Benchmark
  @Group("msConsumerHeavy1P16C")
  @GroupThreads(16)
  public Integer msConsumerHeavy1P16CConsumer(MichaelScottGroupState state,
                                              ConsumerCounters counters) {
    return dequeueOnce(state.queue, counters);
  }


  // =============================================================
  // JDK CLQ - CONSUMER HEAVY
  // =============================================================

  @Benchmark
  @Group("clqConsumerHeavy1P4C")
  @GroupThreads(1)
  public void clqConsumerHeavy1P4CProducer(ClqGroupState state) {
    state.queue.offer(42);
  }

  @Benchmark
  @Group("clqConsumerHeavy1P4C")
  @GroupThreads(4)
  public Integer clqConsumerHeavy1P4CConsumer(ClqGroupState state,
                                              ConsumerCounters counters) {
    return pollOnce(state.queue, counters);
  }


  @Benchmark
  @Group("clqConsumerHeavy1P8C")
  @GroupThreads(1)
  public void clqConsumerHeavy1P8CProducer(ClqGroupState state) {
    state.queue.offer(42);
  }

  @Benchmark
  @Group("clqConsumerHeavy1P8C")
  @GroupThreads(8)
  public Integer clqConsumerHeavy1P8CConsumer(ClqGroupState state,
                                              ConsumerCounters counters) {
    return pollOnce(state.queue, counters);
  }


  @Benchmark
  @Group("clqConsumerHeavy1P16C")
  @GroupThreads(1)
  public void clqConsumerHeavy1P16CProducer(ClqGroupState state) {
    state.queue.offer(42);
  }

  @Benchmark
  @Group("clqConsumerHeavy1P16C")
  @GroupThreads(16)
  public Integer clqConsumerHeavy1P16CConsumer(ClqGroupState state,
                                               ConsumerCounters counters) {
    return pollOnce(state.queue, counters);
  }


  /*
   * -------------------------------------------------------------
   * BURSTY / GROUPED OPERATIONS
   * -------------------------------------------------------------
   *
   * Each worker keeps its own phase:
   *
   *     enqueue enqueue enqueue ...
   *
   * then:
   *
   *     dequeue dequeue dequeue ...
   *
   * Burst length is randomly chosen from 1..16.
   *
   * The queue itself is Scope.Benchmark because every worker
   * participating in this benchmark must hit the same queue.
   */

  @State(Scope.Thread)
  public static class BurstState {

    int remaining;
    boolean enqueuePhase;

    @Setup(Level.Iteration)
    public void setup() {
      remaining = 0;

      // The benchmark toggles phase when a new burst begins, so false here
      // makes the first burst an enqueue burst.
      enqueuePhase = false;
    }

    int nextBurst() {
      return ThreadLocalRandom.current().nextInt(1, 17);
    }
  }


  @State(Scope.Benchmark)
  public static class BasketsBurstState {

    BasketsQueue<Integer> queue;

    @Setup(Level.Iteration)
    public void setup() {
      queue = new BasketsQueue<>(
          MAX_RETRY_ATTEMPTS,
          MAX_JUMPS
      );
    }
  }


  @State(Scope.Benchmark)
  public static class MichaelScottBurstState {

    LockFreeQueue<Integer> queue;

    @Setup(Level.Iteration)
    public void setup() {
      queue = new LockFreeQueue<>();
    }
  }


  @State(Scope.Benchmark)
  public static class ClqBurstState {

    ConcurrentLinkedQueue<Integer> queue;

    @Setup(Level.Iteration)
    public void setup() {
      queue = new ConcurrentLinkedQueue<>();
    }
  }


  @Benchmark
  public Integer basketsGrouped(
      BasketsBurstState queueState,
      BurstState burstState,
      ConsumerCounters counters) {

    if (burstState.remaining == 0) {
      burstState.remaining = burstState.nextBurst();
      burstState.enqueuePhase = !burstState.enqueuePhase;
    }

    Integer result = null;

    if (burstState.enqueuePhase) {
      queueState.queue.enqueue(42);
    } else {
      result = queueState.queue.dequeue();
      recordDequeueResult(result, counters);
    }

    burstState.remaining--;

    return result;
  }


  @Benchmark
  public Integer michaelScottGrouped(
      MichaelScottBurstState queueState,
      BurstState burstState,
      ConsumerCounters counters) {

    if (burstState.remaining == 0) {
      burstState.remaining = burstState.nextBurst();
      burstState.enqueuePhase = !burstState.enqueuePhase;
    }

    Integer result = null;

    if (burstState.enqueuePhase) {
      queueState.queue.enqueue(42);
    } else {
      result = queueState.queue.dequeue();
      recordDequeueResult(result, counters);
    }

    burstState.remaining--;

    return result;
  }


  @Benchmark
  public Integer concurrentLinkedQueueGrouped(
      ClqBurstState queueState,
      BurstState burstState,
      ConsumerCounters counters) {

    if (burstState.remaining == 0) {
      burstState.remaining = burstState.nextBurst();
      burstState.enqueuePhase = !burstState.enqueuePhase;
    }

    Integer result = null;

    if (burstState.enqueuePhase) {
      queueState.queue.offer(42);
    } else {
      result = queueState.queue.poll();
      recordDequeueResult(result, counters);
    }

    burstState.remaining--;

    return result;
  }


  /*
   * -------------------------------------------------------------
   * HELPERS
   * -------------------------------------------------------------
   */

  private static Integer dequeueOnce(
      ConcurrentQueue<Integer> queue,
      ConsumerCounters counters) {

    Integer value = queue.dequeue();
    recordDequeueResult(value, counters);
    return value;
  }


  private static Integer pollOnce(
      ConcurrentLinkedQueue<Integer> queue,
      ConsumerCounters counters) {

    Integer value = queue.poll();
    recordDequeueResult(value, counters);
    return value;
  }


  private static void recordDequeueResult(
      Integer value,
      ConsumerCounters counters) {

    if (value == null) {
      counters.emptyDequeues++;
    } else {
      counters.successfulDequeues++;
    }
  }
}