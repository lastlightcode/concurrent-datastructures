package org.dips.datastructure.stack;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Timeout;

import java.util.EmptyStackException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TreiberEliminationStackTest {

  @RepeatedTest(50)
  @Timeout(30)
  void concurrentPushesAndPopsDoNotLoseOrDuplicateValues()
      throws Exception {

    int producerCount = 4;
    int consumerCount = 4;
    int valuesPerProducer = 25_000;
    int valuesPerConsumer = producerCount * valuesPerProducer / consumerCount;

    int totalValues = producerCount * valuesPerProducer;

    TreiberEliminationStack<Integer> stack = new TreiberEliminationStack<>();

    Set<Integer> poppedValues = ConcurrentHashMap.newKeySet();

    ExecutorService executor = Executors.newFixedThreadPool(producerCount + consumerCount);

    CyclicBarrier startBarrier = new CyclicBarrier(producerCount + consumerCount);

    try {
      Future<?>[] producerFutures = new Future<?>[producerCount];

      Future<?>[] consumerFutures = new Future<?>[consumerCount];

      for (int producer = 0; producer < producerCount; producer++) {
        int producerId = producer;

        producerFutures[producer] = executor.submit(() -> {
          startBarrier.await();

          int start = producerId * valuesPerProducer;
          int end = start + valuesPerProducer;

          for (int value = start; value < end; value++) {
            stack.push(value);
          }

          return null;
        });
      }

      for (int consumer = 0; consumer < consumerCount; consumer++) {
        consumerFutures[consumer] = executor.submit(() -> {
          startBarrier.await();

          int successfulPops = 0;

          while (successfulPops < valuesPerConsumer) {
            try {
              Integer value = stack.pop();

              boolean firstOccurrence =
                  poppedValues.add(value);

              if (!firstOccurrence) {
                throw new AssertionError(
                    "Duplicate value popped: " + value
                );
              }

              successfulPops++;
            } catch (EmptyStackException ignored) {
              /*
               * Producers may not have published another
               * value yet. Retry until this consumer has
               * completed its assigned number of pops.
               */
              Thread.onSpinWait();
            }
          }

          return null;
        });
      }

      for (Future<?> future : producerFutures) {
        future.get(15, SECONDS);
      }

      for (Future<?> future : consumerFutures) {
        future.get(15, SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }

    assertEquals(totalValues, poppedValues.size(), "Every pushed value should be popped exactly once");

    Set<Integer> expectedValues = new HashSet<>(totalValues);

    for (int value = 0; value < totalValues; value++) {
      expectedValues.add(value);
    }

    assertEquals(expectedValues, poppedValues, "The popped values should exactly equal the pushed values");

    /*
     * All produced values have already been consumed, so the stack
     * should now contain no additional element.
     */
    assertTrue(poppedValues.containsAll(expectedValues));
  }
}