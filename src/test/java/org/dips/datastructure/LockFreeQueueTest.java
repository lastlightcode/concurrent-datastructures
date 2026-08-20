package org.dips.datastructure;

import org.dips.datastructure.queue.LockFreeQueue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class LockFreeQueueTest {

  @Test
  void dequeueFromEmptyQueueThrows() {
    LockFreeQueue<Integer> queue = new LockFreeQueue<>();

    assertThrows(NoSuchElementException.class, queue::dequeue);
  }

  @Test
  void enqueueThenDequeueSingleElement() {
    LockFreeQueue<Integer> queue = new LockFreeQueue<>();

    queue.enqueue(42);

    assertEquals(42, queue.dequeue());
    assertThrows(NoSuchElementException.class, queue::dequeue);
  }

  @Test
  void preservesFifoOrderSingleThreaded() {
    LockFreeQueue<Integer> queue = new LockFreeQueue<>();

    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);
    queue.enqueue(4);

    assertEquals(1, queue.dequeue());
    assertEquals(2, queue.dequeue());
    assertEquals(3, queue.dequeue());
    assertEquals(4, queue.dequeue());

    assertThrows(NoSuchElementException.class, queue::dequeue);
  }

  @Test
  void twoProducersDoNotLoseElements() throws Exception {
    LockFreeQueue<Integer> queue = new LockFreeQueue<>();

    int elementsPerProducer = 10_000;

    CountDownLatch start = new CountDownLatch(1);

    Thread producer1 = new Thread(() -> {
      await(start);

      for (int i = 0; i < elementsPerProducer; i++) {
        queue.enqueue(i);
      }
    });

    Thread producer2 = new Thread(() -> {
      await(start);

      for (int i = elementsPerProducer;
           i < elementsPerProducer * 2;
           i++) {
        queue.enqueue(i);
      }
    });

    producer1.start();
    producer2.start();

    start.countDown();

    producer1.join();
    producer2.join();

    Set<Integer> values = new HashSet<>();

    for (int i = 0; i < elementsPerProducer * 2; i++) {
      assertTrue(values.add(queue.dequeue()));
    }

    assertEquals(elementsPerProducer * 2, values.size());
    assertThrows(NoSuchElementException.class, queue::dequeue);
  }

  @Test
  void multipleConsumersCannotRemoveSameElementTwice() throws Exception {
    LockFreeQueue<Integer> queue = new LockFreeQueue<>();

    int elementCount = 20_000;

    for (int i = 0; i < elementCount; i++) {
      queue.enqueue(i);
    }

    List<Integer> consumed =
        Collections.synchronizedList(new ArrayList<>());

    CountDownLatch start = new CountDownLatch(1);

    Runnable consumer = () -> {
      await(start);

      while (true) {
        try {
          consumed.add(queue.dequeue());
        } catch (NoSuchElementException e) {
          return;
        }
      }
    };

    Thread consumer1 = new Thread(consumer);
    Thread consumer2 = new Thread(consumer);
    Thread consumer3 = new Thread(consumer);
    Thread consumer4 = new Thread(consumer);

    consumer1.start();
    consumer2.start();
    consumer3.start();
    consumer4.start();

    start.countDown();

    consumer1.join();
    consumer2.join();
    consumer3.join();
    consumer4.join();

    assertEquals(elementCount, consumed.size());

    Set<Integer> unique = new HashSet<>(consumed);

    assertEquals(elementCount, unique.size());

    for (int i = 0; i < elementCount; i++) {
      assertTrue(unique.contains(i));
    }
  }

  @Test
  void producersAndConsumersRunTogetherWithoutLosingElements()
      throws Exception {

    LockFreeQueue<Integer> queue = new LockFreeQueue<>();

    int producers = 4;
    int consumers = 4;
    int elementsPerProducer = 25_000;
    int totalElements = producers * elementsPerProducer;

    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch producersFinished = new CountDownLatch(producers);

    List<Integer> consumed =
        Collections.synchronizedList(new ArrayList<>());

    ExecutorService executor =
        Executors.newFixedThreadPool(producers + consumers);

    for (int producer = 0; producer < producers; producer++) {
      int producerId = producer;

      executor.submit(() -> {
        await(start);

        int base = producerId * elementsPerProducer;

        for (int i = 0; i < elementsPerProducer; i++) {
          queue.enqueue(base + i);
        }

        producersFinished.countDown();
      });
    }

    for (int consumer = 0; consumer < consumers; consumer++) {
      executor.submit(() -> {
        await(start);

        while (true) {
          try {
            consumed.add(queue.dequeue());
          } catch (NoSuchElementException e) {
            if (producersFinished.getCount() == 0) {
              return;
            }

            Thread.onSpinWait();
          }
        }
      });
    }

    start.countDown();

    executor.shutdown();

    assertTrue(
        executor.awaitTermination(30, TimeUnit.SECONDS),
        "Concurrent test did not terminate"
    );

    assertEquals(totalElements, consumed.size());

    Set<Integer> unique = new HashSet<>(consumed);

    assertEquals(
        totalElements,
        unique.size(),
        "An element was either lost or dequeued more than once"
    );

    for (int i = 0; i < totalElements; i++) {
      assertTrue(unique.contains(i), "Missing value: " + i);
    }
  }

  @Test
  void fifoOrderIsPreservedForEachIndividualProducer()
      throws Exception {

    LockFreeQueue<Integer> queue = new LockFreeQueue<>();

    int count = 10_000;

    CountDownLatch start = new CountDownLatch(1);

    Thread producer = new Thread(() -> {
      await(start);

      for (int i = 0; i < count; i++) {
        queue.enqueue(i);
      }
    });

    producer.start();

    start.countDown();
    producer.join();

    for (int expected = 0; expected < count; expected++) {
      assertEquals(expected, queue.dequeue());
    }
  }

  @Test
  void consumerCanCompleteEnqueueWhenProducerStopsAfterLinking()
      throws Exception {

    CountDownLatch linked = new CountDownLatch(1);
    CountDownLatch resumeProducer = new CountDownLatch(1);

    LockFreeQueue<Integer> queue =
        new LockFreeQueue<>(new LockFreeQueue.Hooks() {
          @Override
          public void afterEnqueueLink() {
            linked.countDown();
            await(resumeProducer);
          }
        });

    Thread producer = new Thread(() -> queue.enqueue(42));

    producer.start();

    // Do not continue until the producer has performed:
    //
    // dummy.next CAS null -> 42
    //
    // but has NOT yet advanced tail.
    assertTrue(linked.await(5, TimeUnit.SECONDS));

    /*
     * We have deliberately constructed:
     *
     * head
     *   |
     *   v
     * [D] -> [42] -> null
     *   ^
     *   |
     * tail
     *
     * Therefore dequeue() must encounter:
     *
     * head == tail
     * head.next != null
     *
     * and help advance tail.
     */

    assertEquals(42, queue.dequeue());

    resumeProducer.countDown();
    producer.join();

    assertThrows(NoSuchElementException.class, queue::dequeue);
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