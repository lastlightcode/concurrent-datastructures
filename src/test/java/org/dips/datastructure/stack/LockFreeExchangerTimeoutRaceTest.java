package org.dips.datastructure.stack;

import org.dips.datastructure.stack.LockFreeExchanger.ExchangeResult;
import org.dips.datastructure.stack.LockFreeExchanger.ExchangeResult.ExchangeSucceeded;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dips.datastructure.stack.LockFreeExchanger.Operation.POP;
import static org.dips.datastructure.stack.LockFreeExchanger.Operation.PUSH;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LockFreeExchangerTimeoutRaceTest {

  @Tag("stress")
  @RepeatedTest(20)
  @Timeout(3)
  void matcherArrivingNearTimeoutCanStillCompleteExchange() throws Exception {

    LockFreeExchanger<Integer> lockFreeExchanger = new LockFreeExchanger<>(1, MILLISECONDS.toNanos(500));

    ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      Future<ExchangeResult<Integer>> push = executor.submit(() -> lockFreeExchanger.visit(PUSH, 42));

      /*
       * Deliberately later than the waiting thread, but with a
       * comfortable enough margin to reduce scheduler flakiness.
       */
      Thread.sleep(400);
      Future<ExchangeResult<Integer>> pop = executor.submit(() -> lockFreeExchanger.visit(POP, null));
      assertEquals(new ExchangeSucceeded<>(42), push.get(2, SECONDS));
      assertEquals(new ExchangeSucceeded<>(42), pop.get(2, SECONDS));
    } finally {
      executor.shutdownNow();
    }
  }

  @RepeatedTest(20)
  @Timeout(10)
  void exchangerNeverDeliversAValueTwice() throws Exception {
    int count = 10_000;

    LockFreeExchanger<Integer> lockFreeExchanger = new LockFreeExchanger<>(4, 10_000);

    Set<Integer> received = ConcurrentHashMap.newKeySet();

    ExecutorService executor = Executors.newFixedThreadPool(8);

    CyclicBarrier barrier = new CyclicBarrier(8);

    try {
      Future<?>[] futures = new Future<?>[8];

      for (int thread = 0; thread < 4; thread++) {
        int producer = thread;

        futures[thread] = executor.submit(() -> {
          barrier.await();

          for (int i = producer; i < count; i += 4) {
            while (true) {
              var result = lockFreeExchanger.visit(PUSH, i);

              if (result instanceof ExchangeSucceeded<Integer>) {
                break;
              }
            }
          }

          return null;
        });
      }

      for (int thread = 0; thread < 4; thread++) {
        futures[4 + thread] = executor.submit(() -> {
          barrier.await();

          int receivedByMe = 0;

          while (receivedByMe < count / 4) {
            var result = lockFreeExchanger.visit(POP, null);
            if (result instanceof ExchangeSucceeded<Integer>(Integer value)) {
              if (!received.add(value)) {
                throw new AssertionError("Duplicate exchange: " + value);
              }

              receivedByMe++;
            }
          }

          return null;
        });
      }

      for (Future<?> future : futures) {
        future.get(8, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }

    assertEquals(count, received.size());
  }
}