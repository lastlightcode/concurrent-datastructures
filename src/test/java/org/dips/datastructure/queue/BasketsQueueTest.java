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
}