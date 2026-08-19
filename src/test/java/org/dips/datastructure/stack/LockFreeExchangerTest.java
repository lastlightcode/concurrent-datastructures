package org.dips.datastructure.stack;

import org.dips.datastructure.stack.LockFreeExchanger.ExchangeResult;
import org.dips.datastructure.stack.LockFreeExchanger.ExchangeResult.ExchangeFailed;
import org.dips.datastructure.stack.LockFreeExchanger.ExchangeResult.ExchangeSucceeded;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dips.datastructure.stack.LockFreeExchanger.Operation.POP;
import static org.dips.datastructure.stack.LockFreeExchanger.Operation.PUSH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class LockFreeExchangerTest {

  @Test
  @Timeout(3)
  void pushAndPopCanEliminate() throws Exception {
    LockFreeExchanger<Integer> lockFreeExchanger =
        new LockFreeExchanger<>(1, MILLISECONDS.toNanos(500));

    Pair<ExchangeResult<Integer>> results =
        runTogether(
            () -> lockFreeExchanger.visit(PUSH, 42),
            () -> lockFreeExchanger.visit(POP, null)
        );

    assertEquals(
        new ExchangeSucceeded<>(42),
        results.first()
    );

    assertEquals(
        new ExchangeSucceeded<>(42),
        results.second()
    );
  }

  @Test
  @Timeout(3)
  void twoPushesCannotEliminate() throws Exception {
    LockFreeExchanger<Integer> lockFreeExchanger =
        new LockFreeExchanger<>(1, MILLISECONDS.toNanos(100));

    Pair<ExchangeResult<Integer>> results =
        runTogether(
            () -> lockFreeExchanger.visit(PUSH, 10),
            () -> lockFreeExchanger.visit(PUSH, 20)
        );

    assertInstanceOf(
        ExchangeFailed.class,
        results.first()
    );

    assertInstanceOf(
        ExchangeFailed.class,
        results.second()
    );
  }

  @Test
  @Timeout(3)
  void twoPopsCannotEliminate() throws Exception {
    LockFreeExchanger<Integer> lockFreeExchanger =
        new LockFreeExchanger<>(1, MILLISECONDS.toNanos(100));

    Pair<ExchangeResult<Integer>> results =
        runTogether(
            () -> lockFreeExchanger.visit(POP, null),
            () -> lockFreeExchanger.visit(POP, null)
        );

    assertInstanceOf(
        ExchangeFailed.class,
        results.first()
    );

    assertInstanceOf(
        ExchangeFailed.class,
        results.second()
    );
  }

  @Test
  @Timeout(3)
  void timedOutSlotCanBeReused() throws Exception {
    LockFreeExchanger<Integer> lockFreeExchanger =
        new LockFreeExchanger<>(1, MILLISECONDS.toNanos(10));

    /*
     * No matching pop exists, so this push must time out and
     * withdraw its WAITING exchange from the slot.
     */
    ExchangeResult<Integer> timeoutResult =
        lockFreeExchanger.visit(PUSH, 10);

    assertInstanceOf(
        ExchangeFailed.class,
        timeoutResult
    );

    /*
     * Reuse the same one-slot exchanger. If the timed-out push
     * left WAITING or BUSY behind, this pair may not succeed.
     */
    Pair<ExchangeResult<Integer>> results =
        runTogether(
            () -> lockFreeExchanger.visit(PUSH, 42),
            () -> lockFreeExchanger.visit(POP, null)
        );

    assertEquals(
        new ExchangeSucceeded<>(42),
        results.first()
    );

    assertEquals(
        new ExchangeSucceeded<>(42),
        results.second()
    );
  }

  private static <T> Pair<T> runTogether(
      ThrowingSupplier<T> firstOperation,
      ThrowingSupplier<T> secondOperation
  ) throws Exception {

    ExecutorService executor = Executors.newFixedThreadPool(2);
    CyclicBarrier startBarrier = new CyclicBarrier(2);

    try {
      Future<T> first = executor.submit(() -> {
        startBarrier.await();
        return firstOperation.get();
      });

      Future<T> second = executor.submit(() -> {
        startBarrier.await();
        return secondOperation.get();
      });

      /*
       * These bounds prevent a broken exchanger from hanging
       * the test suite indefinitely.
       */
      T firstResult = first.get(2, SECONDS);
      T secondResult = second.get(2, SECONDS);

      return new Pair<>(firstResult, secondResult);
    } finally {
      executor.shutdownNow();
    }
  }

  @FunctionalInterface
  private interface ThrowingSupplier<T> {
    T get() throws Exception;
  }

  private record Pair<T>(T first, T second) { }
}