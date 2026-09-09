package org.dips.datastructure.queue;

import org.dips.datastructure.queue.BasketsQueue.DequeueProbe;
import org.dips.datastructure.queue.BasketsQueue.EnqueueProbe;
import org.dips.datastructure.queue.BasketsQueue.Node;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

  @Test
  void onlyOneConsumerCanClaimSameLiveCandidate() throws Exception {
    var bothAtMarkCas = new CountDownLatch(2);
    var releaseConsumers = new CountDownLatch(1);

    DequeueProbe<Integer> dequeueProbe = new DequeueProbe<>() {

      @Override
      public void beforeMarkCas(
          Node<Integer> current,
          Node<Integer> candidate) {

        bothAtMarkCas.countDown();
        await(releaseConsumers);
      }
    };

    var queue = new BasketsQueue<>(
        16,
        16,
        new EnqueueProbe<>() {},
        dequeueProbe
    );

    queue.enqueue(42);

    var result1 = new AtomicReference<Integer>();
    var result2 = new AtomicReference<Integer>();

    var c1 = Thread.ofPlatform().start(() ->
        result1.set(queue.dequeue())
    );

    var c2 = Thread.ofPlatform().start(() ->
        result2.set(queue.dequeue())
    );

    assertTrue(
        bothAtMarkCas.await(5, TimeUnit.SECONDS),
        "Both consumers should observe the live candidate before either CASes"
    );

    releaseConsumers.countDown();

    c1.join();
    c2.join();

    var results = Arrays.asList(result1.get(), result2.get());

    assertEquals(
        1,
        results.stream()
            .filter(Objects::nonNull)
            .count(),
        "Exactly one consumer should dequeue the element"
    );

    assertTrue(
        results.contains(42),
        "The successfully dequeued value should be 42"
    );

    assertNull(queue.dequeue());
  }

  @Test
  void producerAppendRemainsReachableWhileConsumerPausedBeforeHeadCleanup() throws Exception {
    var consumerMarkedCandidate = new CountDownLatch(1);
    var allowConsumerToContinue = new CountDownLatch(1);

    DequeueProbe<Integer> dequeueProbe = new DequeueProbe<>() {
      @Override
      public void afterMarkCas(
          BasketsQueue.Node<Integer> current,
          BasketsQueue.Node<Integer> candidate) {

        consumerMarkedCandidate.countDown();
        await(allowConsumerToContinue);
      }
    };

    var queue = new BasketsQueue<Integer>(
        0,
        16,
        new EnqueueProbe<>() {},
        dequeueProbe
    );

    queue.enqueue(1);

    var dequeued = new AtomicReference<Integer>();

    var consumer = Thread.ofPlatform().start(() ->
        dequeued.set(queue.dequeue())
    );

    assertTrue(
        consumerMarkedCandidate.await(5, TimeUnit.SECONDS),
        "Consumer should mark the candidate before being released"
    );

    // Consumer has logically deleted 1,
    // but has not yet advanced head.
    queue.enqueue(2);

    allowConsumerToContinue.countDown();

    consumer.join();

    assertEquals(1, dequeued.get());

    assertEquals(
        2,
        queue.dequeue(),
        "Element appended while consumer was paused must remain reachable"
    );

    assertNull(queue.dequeue());
  }

  @Test
  void staleBasketProducerCannotPublishBehindAdvancedHead() throws Exception {
    var loserBeforeOrdinaryCas = new CountDownLatch(1);
    var allowLoserOrdinaryCas = new CountDownLatch(1);

    var winnerAfterOrdinaryLink = new CountDownLatch(1);
    var allowWinnerTailUpdate = new CountDownLatch(1);

    var loserBeforeBasketCas = new CountDownLatch(1);
    var allowLoserBasketCas = new CountDownLatch(1);

    var consumerAfterMarkCas = new CountDownLatch(1);
    var allowConsumerCleanup = new CountDownLatch(1);

    var ordinaryCalls = new AtomicInteger();

    EnqueueProbe<Integer> enqueueProbe = new EnqueueProbe<>() {

      @Override
      public void beforeOrdinaryLink(
          BasketsQueue.Node<Integer> observedTail,
          BasketsQueue.Node<Integer> node) {

        int call = ordinaryCalls.incrementAndGet();

        if (call == 1) {
          loserBeforeOrdinaryCas.countDown();
          await(allowLoserOrdinaryCas);
        }
      }

      @Override
      public void afterOrdinaryLink(
          BasketsQueue.Node<Integer> observedTail,
          BasketsQueue.Node<Integer> node) {

        winnerAfterOrdinaryLink.countDown();
        await(allowWinnerTailUpdate);
      }

      @Override
      public void beforeBasketCas(
          BasketsQueue.Node<Integer> observedTail,
          BasketsQueue.Node<Integer> current,
          BasketsQueue.Node<Integer> node) {

        loserBeforeBasketCas.countDown();
        await(allowLoserBasketCas);
      }
    };

    DequeueProbe<Integer> dequeueProbe = new DequeueProbe<>() {
      @Override
      public void afterMarkCas(
          BasketsQueue.Node<Integer> current,
          BasketsQueue.Node<Integer> candidate) {

        consumerAfterMarkCas.countDown();
        await(allowConsumerCleanup);
      }
    };

    var queue = new BasketsQueue<Integer>(
        16,
        16,
        enqueueProbe,
        dequeueProbe
    );

    var loser = Thread.ofPlatform().start(() ->
        queue.enqueue(2)
    );

    assertTrue(
        loserBeforeOrdinaryCas.await(5, TimeUnit.SECONDS),
        "Loser should observe null before ordinary CAS"
    );

    var winner = Thread.ofPlatform().start(() ->
        queue.enqueue(1)
    );

    assertTrue(
        winnerAfterOrdinaryLink.await(5, TimeUnit.SECONDS),
        "Winner should publish before updating tail"
    );

    allowLoserOrdinaryCas.countDown();

    assertTrue(
        loserBeforeBasketCas.await(5, TimeUnit.SECONDS),
        "Loser should reach stale basket CAS"
    );

    var result = new AtomicReference<Integer>();

    var consumer = Thread.ofPlatform().start(() ->
        result.set(queue.dequeue())
    );

    assertTrue(
        consumerAfterMarkCas.await(5, TimeUnit.SECONDS),
        "Consumer should mark the live candidate"
    );

    allowConsumerCleanup.countDown();
    consumer.join();

    assertEquals(1, result.get());

    allowLoserBasketCas.countDown();
    allowWinnerTailUpdate.countDown();

    loser.join();
    winner.join();

    assertEquals(
        2,
        queue.dequeue(),
        "Stale producer must not publish behind the reclaimed frontier"
    );

    assertNull(queue.dequeue());
  }

  @Test
  void twoConsumersRacingToAdvanceSameHeadDoNotLoseElements() throws Exception {
    var bothBeforeHeadCleanup = new CountDownLatch(2);
    var allowCleanup = new CountDownLatch(1);

    DequeueProbe<Integer> dequeueProbe = new DequeueProbe<>() {
      @Override
      public void beforeHeadCleanup(
          BasketsQueue.Node<Integer> observedHead,
          BasketsQueue.Node<Integer> cleanupTarget) {

        bothBeforeHeadCleanup.countDown();
        await(allowCleanup);
      }
    };

    var queue = new BasketsQueue<Integer>(
        16,
        2,
        new EnqueueProbe<>() {},
        dequeueProbe
    );

    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);
    queue.enqueue(4);

    // Create a dead prefix first.
    assertEquals(1, queue.dequeue());
    assertEquals(2, queue.dequeue());

    var r1 = new AtomicReference<Integer>();
    var r2 = new AtomicReference<Integer>();

    var c1 = Thread.ofPlatform().start(() ->
        r1.set(queue.dequeue())
    );

    var c2 = Thread.ofPlatform().start(() ->
        r2.set(queue.dequeue())
    );

    assertTrue(
        bothBeforeHeadCleanup.await(5, TimeUnit.SECONDS),
        "Both consumers should reach head cleanup from the same old frontier"
    );

    allowCleanup.countDown();

    c1.join();
    c2.join();

    assertNotNull(r1.get());
    assertNotNull(r2.get());

    assertNotEquals(
        r1.get(),
        r2.get(),
        "Consumers must not return the same element"
    );

    assertEquals(
        Set.of(3, 4),
        Set.of(r1.get(), r2.get())
    );

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