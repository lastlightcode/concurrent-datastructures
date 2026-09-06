package org.dips.datastructure.queue;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.dips.datastructure.queue.BasketsQueue.EnqueueProbe;
import org.junit.jupiter.api.Test;

class BasketsQueueTest {

  @Test
  void singleProducerEnqueuesAllElements() {
    var queue = new BasketsQueue<Integer>(16);

    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);

    // Once dequeue exists:
    // assertEquals(1, queue.dequeue());
    // assertEquals(2, queue.dequeue());
    // assertEquals(3, queue.dequeue());
  }

  @Test
  void manyConcurrentProducersAllComplete() throws Exception {
    int producers = 8;
    int elementsPerProducer = 10_000;

    var queue = new BasketsQueue<Integer>(16);

    try (var executor = Executors.newFixedThreadPool(producers)) {
      var start = new CountDownLatch(1);
      var done = new CountDownLatch(producers);

      for (int producer = 0; producer < producers; producer++) {
        int producerId = producer;

        executor.submit(() -> {
          try {
            start.await();

            for (int i = 0; i < elementsPerProducer; i++) {
              queue.enqueue(producerId * elementsPerProducer + i);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            done.countDown();
          }
        });
      }

      start.countDown();

      assertTrue(done.await(10, TimeUnit.SECONDS));
    }
  }

  /**
   * Freeze the ordinary tail winner before the update
   */
  @Test
  void staleTailIsRepairedPastBasketMembers() throws Exception {
    var winnerLinked = new CountDownLatch(1);
    var allowWinnerToContinue = new CountDownLatch(1);

    var queue = new BasketsQueue<Integer>(
        16,
        new EnqueueProbe<>() {
          @Override
          public void afterOrdinaryLink(
              BasketsQueue.Node<Integer> observedTail,
              BasketsQueue.Node<Integer> node) {

            if (node.value == 1) {
              winnerLinked.countDown();

              try {
                allowWinnerToContinue.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }
          }
        });

    var winner = Thread.ofPlatform().start(() -> queue.enqueue(1));

    assertTrue(winnerLinked.await(5, TimeUnit.SECONDS));

    var c = Thread.ofPlatform().start(() -> queue.enqueue(2));
    var d = Thread.ofPlatform().start(() -> queue.enqueue(3));

    c.join();
    d.join();

    // Another producer should encounter the stale tail and repair it.
    var e = Thread.ofPlatform().start(() -> queue.enqueue(4));
    e.join();

    allowWinnerToContinue.countDown();
    winner.join();
  }

  /**
   * Basket insertion must not advance tail
   */
  @Test
  void basketInsertionDoesNotAdvanceTailToBasketMember() throws Exception {
    var twoReadyToCas = new CountDownLatch(1);
    var allowTwoToCas = new CountDownLatch(1);

    var oneLinked = new CountDownLatch(1);
    var allowOneToAdvanceTail = new CountDownLatch(1);

    var queue = new BasketsQueue<Integer>(
        16,
        new EnqueueProbe<>() {

          @Override
          public void beforeOrdinaryLink(
              BasketsQueue.Node<Integer> observedTail,
              BasketsQueue.Node<Integer> node) {

            if (node.value == 2) {
              twoReadyToCas.countDown();
              await(allowTwoToCas);
            }
          }

          @Override
          public void afterOrdinaryLink(
              BasketsQueue.Node<Integer> observedTail,
              BasketsQueue.Node<Integer> node) {

            if (node.value == 1) {
              oneLinked.countDown();
              await(allowOneToAdvanceTail);
            }
          }
        });

    // Start 2 first.
    // It observes sentinel.next == null, then freezes BEFORE its CAS.
    Thread two = Thread.ofPlatform().start(() -> queue.enqueue(2));

    assertTrue(twoReadyToCas.await(5, TimeUnit.SECONDS));

    // Now 1 observes the same null and wins the CAS.
    Thread one = Thread.ofPlatform().start(() -> queue.enqueue(1));

    // 1 has linked, but has NOT advanced tail.
    assertTrue(oneLinked.await(5, TimeUnit.SECONDS));

    /*
     * Current state:
     *
     * sentinel -> 1
     * ^
     * tail
     *
     * Thread 2 is still holding an old observation:
     * sentinel.next == null
     */

    // Wake 2.
    // Its CAS(null -> 2) must now fail, forcing it into basket insertion.
    allowTwoToCas.countDown();

    two.join();

    /*
     * Now we expect:
     *
     * sentinel -> 2 -> 1
     * ^
     * tail
     *
     * 2 is genuinely a basket member and must NOT have advanced tail.
     */
    assertNotEquals(
        Integer.valueOf(2),
        queue.tailNode().value,
        "basket member must not become tail");

    // Let 1 perform its best-effort tail update.
    allowOneToAdvanceTail.countDown();
    one.join();
  }

  /**
   * Retry exhaustion must not carry stale linkage
   */
  @Test
  void exhaustedBasketRetryClearsUnpublishedNodeLink() throws Exception {

    var cReadyForOrdinaryCas = new CountDownLatch(1);
    var dReadyForOrdinaryCas = new CountDownLatch(1);

    var allowCOrdinaryCas = new CountDownLatch(1);
    var allowDOrdinaryCas = new CountDownLatch(1);

    var bLinked = new CountDownLatch(1);
    var allowBToAdvanceTail = new CountDownLatch(1);

    var cReadyForBasketCas = new CountDownLatch(1);
    var allowCBasketCas = new CountDownLatch(1);

    var exhausted = new CountDownLatch(1);

    AtomicReference<BasketsQueue.Node<Integer>> abandoned =
        new AtomicReference<>();

    var queue = new BasketsQueue<Integer>(
        1,
        new EnqueueProbe<>() {

          @Override
          public void beforeOrdinaryLink(
              BasketsQueue.Node<Integer> observedTail,
              BasketsQueue.Node<Integer> node) {

            if (node.value == 3) {
              cReadyForOrdinaryCas.countDown();
              await(allowCOrdinaryCas);
            }

            if (node.value == 2) {
              dReadyForOrdinaryCas.countDown();
              await(allowDOrdinaryCas);
            }
          }

          @Override
          public void afterOrdinaryLink(
              BasketsQueue.Node<Integer> observedTail,
              BasketsQueue.Node<Integer> node) {

            if (node.value == 1) {
              bLinked.countDown();
              await(allowBToAdvanceTail);
            }
          }

          @Override
          public void beforeBasketCas(
              BasketsQueue.Node<Integer> observedTail,
              BasketsQueue.Node<Integer> current,
              BasketsQueue.Node<Integer> node) {

            if (node.value == 3) {
              cReadyForBasketCas.countDown();
              await(allowCBasketCas);
            }
          }

          @Override
          public void afterBasketRetryExhausted(
              BasketsQueue.Node<Integer> node) {

            if (node.value == 3) {
              abandoned.set(node);
              exhausted.countDown();
            }
          }
        });

    /*
     * C observes:
     *
     * A.next == null
     *
     * and freezes before CAS(null -> C).
     */
    Thread c = Thread.ofPlatform().start(() -> queue.enqueue(3));

    assertTrue(
        cReadyForOrdinaryCas.await(5, TimeUnit.SECONDS),
        "C never reached ordinary CAS");

    /*
     * D ALSO observes:
     *
     * A.next == null
     *
     * and freezes before CAS(null -> D).
     */
    Thread d = Thread.ofPlatform().start(() -> queue.enqueue(2));

    assertTrue(
        dReadyForOrdinaryCas.await(5, TimeUnit.SECONDS),
        "D never reached ordinary CAS");

    /*
     * Now B gets to win the ordinary append.
     */
    Thread b = Thread.ofPlatform().start(() -> queue.enqueue(1));

    assertTrue(
        bLinked.await(5, TimeUnit.SECONDS),
        "B never linked");

    /*
     * Structure:
     *
     * A -> B
     * ^
     * tail
     *
     * Both C and D still believe A.next was null.
     */

    /*
     * Wake C first.
     *
     * C's ordinary CAS fails.
     * It enters basket insertion, reads current = B,
     * sets C.next = B, then freezes before its basket CAS.
     */
    allowCOrdinaryCas.countDown();

    assertTrue(
        cReadyForBasketCas.await(5, TimeUnit.SECONDS),
        "C never reached basket CAS");

    /*
     * C is now prepared to do:
     *
     * CAS(A.next, B, C)
     *
     * but we keep it frozen.
     */

    /*
     * Wake D.
     *
     * D's ordinary CAS(null -> D) fails.
     *
     * D then enters the basket and successfully changes:
     *
     * A.next: B -> D
     *
     * giving:
     *
     * A -> D -> B
     */
    allowDOrdinaryCas.countDown();

    d.join();

    /*
     * Now C's expected value B is stale.
     */
    allowCBasketCas.countDown();

    /*
     * C must fail its basket CAS.
     *
     * MAX_RETRY_ATTEMPTS == 1, so it should abandon
     * this basket attempt and clear C.next.
     */
    assertTrue(
        exhausted.await(5, TimeUnit.SECONDS),
        "C never exhausted its basket retry");

    assertNotNull(abandoned.get());

    assertNull(
        abandoned.get().next.getReference(),
        "abandoned unpublished node must clear stale basket linkage");

    /*
     * Finally release B so everything can finish.
     */
    allowBToAdvanceTail.countDown();

    b.join();
    c.join();
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }
}