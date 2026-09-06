package org.dips.datastructure.queue;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
        new BasketsQueue.EnqueueProbe<>() {
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
}