package org.dips.datastructure.queue;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BasketsQueueTest {

  @Test
  void singleProducerEnqueuesAllElements() {
    var queue = new BasketsQueue<Integer>(16, 16);

    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);

    // Once dequeue exists:
    assertEquals(1, queue.dequeue());
    assertEquals(2, queue.dequeue());
    assertEquals(3, queue.dequeue());
  }

  @Test
  void manyConcurrentProducersAllComplete() throws Exception {
    int producers = 8;
    int elementsPerProducer = 10_000;

    var queue = new BasketsQueue<Integer>(16, 16);

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

  @Test
  void emptyQueueReturnsNull() {
    var queue = new BasketsQueue<Integer>(16, 16);

    assertNull(queue.dequeue());
  }

  @Test
  void singleElementCanBeDequeued() {
    var queue = new BasketsQueue<Integer>(16, 16);

    queue.enqueue(42);

    assertEquals(42, queue.dequeue());
    assertNull(queue.dequeue());
  }

  @Test
  void preservesFifoForSequentialOperations() {
    var queue = new BasketsQueue<Integer>(16, 16);

    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);

    assertEquals(1, queue.dequeue());
    assertEquals(2, queue.dequeue());
    assertEquals(3, queue.dequeue());
    assertNull(queue.dequeue());
  }

  @Test
  void canReuseQueueAfterBecomingEmpty() {
    var queue = new BasketsQueue<Integer>(16, 16);

    queue.enqueue(1);
    assertEquals(1, queue.dequeue());
    assertNull(queue.dequeue());

    queue.enqueue(2);
    assertEquals(2, queue.dequeue());
    assertNull(queue.dequeue());
  }

  @Test
  void twoConsumersDoNotReturnSameElement() throws Exception {
    var queue = new BasketsQueue<Integer>(16, 16);

    queue.enqueue(1);
    queue.enqueue(2);

    var start = new CountDownLatch(1);

    var result1 = new AtomicReference<Integer>();
    var result2 = new AtomicReference<Integer>();

    var t1 = Thread.ofPlatform().start(() -> {
      await(start);
      result1.set(queue.dequeue());
    });

    var t2 = Thread.ofPlatform().start(() -> {
      await(start);
      result2.set(queue.dequeue());
    });

    start.countDown();

    t1.join();
    t2.join();

    assertNotNull(result1.get());
    assertNotNull(result2.get());

    assertNotEquals(result1.get(), result2.get());

    assertEquals(
        Set.of(1, 2),
        Set.of(result1.get(), result2.get())
    );

    assertNull(queue.dequeue());
  }

  @Test
  void repeatedDequeuesCanTraverseDeletedPrefix() {
    var queue = new BasketsQueue<Integer>(16, 16);

    for (int i = 0; i < 100; i++) {
      queue.enqueue(i);
    }

    for (int i = 0; i < 99; i++) {
      assertEquals(i, queue.dequeue());
    }

    assertEquals(99, queue.dequeue());
    assertNull(queue.dequeue());
  }

  @Test
  void smallCleanupThresholdDoesNotLoseElements() {
    var queue = new BasketsQueue<Integer>(2, 16);

    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);
    queue.enqueue(4);
    queue.enqueue(5);

    assertEquals(1, queue.dequeue());
    assertEquals(2, queue.dequeue());
    assertEquals(3, queue.dequeue());
    assertEquals(4, queue.dequeue());
    assertEquals(5, queue.dequeue());

    assertNull(queue.dequeue());
  }

  @Test
  void enqueueAfterSeveralDequeuesRemainsReachable() {
    var queue = new BasketsQueue<Integer>(2, 16);

    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);

    assertEquals(1, queue.dequeue());
    assertEquals(2, queue.dequeue());

    queue.enqueue(4);
    queue.enqueue(5);

    assertEquals(3, queue.dequeue());
    assertEquals(4, queue.dequeue());
    assertEquals(5, queue.dequeue());

    assertNull(queue.dequeue());
  }

  @Test
  void concurrentProducerAndConsumerDoNotLoseElements() throws Exception {
    var queue = new BasketsQueue<Integer>(4, 16);

    int count = 10_000;

    var start = new CountDownLatch(1);
    var consumed = ConcurrentHashMap.<Integer>newKeySet();

    var producer = Thread.ofPlatform().start(() -> {
      await(start);

      for (int i = 0; i < count; i++) {
        queue.enqueue(i);
      }
    });

    var consumer = Thread.ofPlatform().start(() -> {
      await(start);

      while (consumed.size() < count) {
        Integer value = queue.dequeue();

        if (value != null) {
          assertTrue(
              consumed.add(value),
              "Duplicate dequeue: " + value
          );
        }
      }
    });

    start.countDown();

    producer.join();
    consumer.join();

    assertEquals(count, consumed.size());

    for (int i = 0; i < count; i++) {
      assertTrue(consumed.contains(i));
    }

    assertNull(queue.dequeue());
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