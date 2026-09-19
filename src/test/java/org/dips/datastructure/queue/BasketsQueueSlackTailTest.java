package org.dips.datastructure.queue;

import org.dips.datastructure.queue.BasketsQueueSlackTail.DequeueProbe;
import org.dips.datastructure.queue.BasketsQueueSlackTail.EnqueueProbe;
import org.dips.datastructure.queue.BasketsQueueSlackTail.Node;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BasketsQueueSlackTailTest {

  @Test
  void singleProducerEnqueuesAllElements() {
    var queue = new BasketsQueueSlackTail<>(16, 16);

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

    var queue = new BasketsQueueSlackTail<Integer>(16, 16);

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
    var queue = new BasketsQueueSlackTail<Integer>(16, 16);

    assertNull(queue.dequeue());
  }

  @Test
  void singleElementCanBeDequeued() {
    var queue = new BasketsQueueSlackTail<Integer>(16, 16);

    queue.enqueue(42);

    assertEquals(42, queue.dequeue());
    assertNull(queue.dequeue());
  }

  @Test
  void preservesFifoForSequentialOperations() {
    var queue = new BasketsQueueSlackTail<Integer>(16, 16);

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
    var queue = new BasketsQueueSlackTail<Integer>(16, 16);

    queue.enqueue(1);
    assertEquals(1, queue.dequeue());
    assertNull(queue.dequeue());

    queue.enqueue(2);
    assertEquals(2, queue.dequeue());
    assertNull(queue.dequeue());
  }

  @Test
  void twoConsumersDoNotReturnSameElement() throws Exception {
    var queue = new BasketsQueueSlackTail<Integer>(16, 16);

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

    assertEquals(Set.of(1, 2), Set.of(result1.get(), result2.get()));

    assertNull(queue.dequeue());
  }

  @Test
  void repeatedDequeuesCanTraverseDeletedPrefix() {
    var queue = new BasketsQueueSlackTail<Integer>(16, 16);

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
    var queue = new BasketsQueueSlackTail<Integer>(2, 16);

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
    var queue = new BasketsQueueSlackTail<Integer>(2, 16);

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
    var queue = new BasketsQueueSlackTail<Integer>(4, 16);

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
          assertTrue(consumed.add(value), "Duplicate dequeue: " + value);
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
      public void beforeMarkCas(Node<Integer> current, Node<Integer> candidate) {
        bothAtMarkCas.countDown();
        await(releaseConsumers);
      }
    };

    var queue = new BasketsQueueSlackTail<>(16, 16, new EnqueueProbe<>() {
    }, dequeueProbe);

    queue.enqueue(42);

    var result1 = new AtomicReference<Integer>();
    var result2 = new AtomicReference<Integer>();

    var c1 = Thread.ofPlatform().start(() -> result1.set(queue.dequeue()));

    var c2 = Thread.ofPlatform().start(() -> result2.set(queue.dequeue()));

    assertTrue(bothAtMarkCas.await(5, TimeUnit.SECONDS), "Both consumers should observe the live candidate before either CASes");

    releaseConsumers.countDown();

    c1.join();
    c2.join();

    var results = Arrays.asList(result1.get(), result2.get());

    assertEquals(1, results.stream().filter(Objects::nonNull).count(), "Exactly one consumer should dequeue the element");

    assertTrue(results.contains(42), "The successfully dequeued value should be 42");

    assertNull(queue.dequeue());
  }

  @Test
  void producerAppendRemainsReachableWhileConsumerPausedBeforeHeadCleanup() throws Exception {
    var consumerMarkedCandidate = new CountDownLatch(1);
    var allowConsumerToContinue = new CountDownLatch(1);

    DequeueProbe<Integer> dequeueProbe = new DequeueProbe<>() {
      @Override
      public void afterMarkCas(Node<Integer> current, Node<Integer> candidate) {

        consumerMarkedCandidate.countDown();
        await(allowConsumerToContinue);
      }
    };

    var queue = new BasketsQueueSlackTail<Integer>(0, 16, new EnqueueProbe<>() {
    }, dequeueProbe);

    queue.enqueue(1);

    var dequeued = new AtomicReference<Integer>();

    var consumer = Thread.ofPlatform().start(() -> dequeued.set(queue.dequeue()));

    assertTrue(consumerMarkedCandidate.await(5, TimeUnit.SECONDS), "Consumer should mark the candidate before being released");

    // Consumer has logically deleted 1,
    // but has not yet advanced head.
    queue.enqueue(2);

    allowConsumerToContinue.countDown();

    consumer.join();

    assertEquals(1, dequeued.get());

    assertEquals(2, queue.dequeue(), "Element appended while consumer was paused must remain reachable");

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
      public void beforeOrdinaryLink(Node<Integer> observedTail, Node<Integer> node) {

        int call = ordinaryCalls.incrementAndGet();

        if (call == 1) {
          loserBeforeOrdinaryCas.countDown();
          await(allowLoserOrdinaryCas);
        }
      }

      @Override
      public void afterOrdinaryLink(Node<Integer> observedTail, Node<Integer> node) {

        winnerAfterOrdinaryLink.countDown();
        await(allowWinnerTailUpdate);
      }

      @Override
      public void beforeBasketCas(Node<Integer> observedTail, Node<Integer> current, Node<Integer> node) {

        loserBeforeBasketCas.countDown();
        await(allowLoserBasketCas);
      }
    };

    DequeueProbe<Integer> dequeueProbe = new DequeueProbe<>() {
      @Override
      public void afterMarkCas(Node<Integer> current, Node<Integer> candidate) {

        consumerAfterMarkCas.countDown();
        await(allowConsumerCleanup);
      }
    };

    var queue = new BasketsQueueSlackTail<Integer>(16, 16, enqueueProbe, dequeueProbe);

    var loser = Thread.ofPlatform().start(() -> queue.enqueue(2));

    assertTrue(loserBeforeOrdinaryCas.await(5, TimeUnit.SECONDS), "Loser should observe null before ordinary CAS");

    var winner = Thread.ofPlatform().start(() -> queue.enqueue(1));

    assertTrue(winnerAfterOrdinaryLink.await(5, TimeUnit.SECONDS), "Winner should publish before updating tail");

    allowLoserOrdinaryCas.countDown();

    assertTrue(loserBeforeBasketCas.await(5, TimeUnit.SECONDS), "Loser should reach stale basket CAS");

    var result = new AtomicReference<Integer>();

    var consumer = Thread.ofPlatform().start(() -> result.set(queue.dequeue()));

    assertTrue(consumerAfterMarkCas.await(5, TimeUnit.SECONDS), "Consumer should mark the live candidate");

    allowConsumerCleanup.countDown();
    consumer.join();

    assertEquals(1, result.get());

    allowLoserBasketCas.countDown();
    allowWinnerTailUpdate.countDown();

    loser.join();
    winner.join();

    assertEquals(2, queue.dequeue(), "Stale producer must not publish behind the reclaimed frontier");

    assertNull(queue.dequeue());
  }

  @Test
  void twoConsumersRacingToAdvanceSameHeadDoNotLoseElements() throws Exception {
    var bothBeforeHeadCleanup = new CountDownLatch(2);
    var allowCleanup = new CountDownLatch(1);

    DequeueProbe<Integer> dequeueProbe = new DequeueProbe<>() {
      @Override
      public void beforeHeadCleanup(Node<Integer> observedHead, Node<Integer> cleanupTarget) {

        bothBeforeHeadCleanup.countDown();
        await(allowCleanup);
      }
    };

    var queue = new BasketsQueueSlackTail<Integer>(16, 2, new EnqueueProbe<>() {
    }, dequeueProbe);

    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);
    queue.enqueue(4);

    // Create a dead prefix first.
    assertEquals(1, queue.dequeue());
    assertEquals(2, queue.dequeue());

    var r1 = new AtomicReference<Integer>();
    var r2 = new AtomicReference<Integer>();

    var c1 = Thread.ofPlatform().start(() -> r1.set(queue.dequeue()));

    var c2 = Thread.ofPlatform().start(() -> r2.set(queue.dequeue()));

    assertTrue(bothBeforeHeadCleanup.await(5, TimeUnit.SECONDS), "Both consumers should reach head cleanup from the same old frontier");

    allowCleanup.countDown();

    c1.join();
    c2.join();

    assertNotNull(r1.get());
    assertNotNull(r2.get());

    assertNotEquals(r1.get(), r2.get(), "Consumers must not return the same element");

    assertEquals(Set.of(3, 4), Set.of(r1.get(), r2.get()));

    assertNull(queue.dequeue());
  }

  @Test
  void markedLinkCannotBeChangedUsingStaleUnmarkedExpectation() throws Exception {
    var consumerAfterMark = new CountDownLatch(1);
    var allowConsumerToReturn = new CountDownLatch(1);

    var capturedCurrent = new AtomicReference<Node<Integer>>();

    var capturedCandidate = new AtomicReference<Node<Integer>>();

    DequeueProbe<Integer> dequeueProbe = new DequeueProbe<>() {
      @Override
      public void afterMarkCas(Node<Integer> current, Node<Integer> candidate) {

        capturedCurrent.set(current);
        capturedCandidate.set(candidate);

        consumerAfterMark.countDown();
        await(allowConsumerToReturn);
      }
    };

    var queue = new BasketsQueueSlackTail<Integer>(16, 16, new EnqueueProbe<>() {
    }, dequeueProbe);

    queue.enqueue(42);

    var result = new AtomicReference<Integer>();

    var consumer = Thread.ofPlatform().start(() -> result.set(queue.dequeue()));

    assertTrue(consumerAfterMark.await(5, TimeUnit.SECONDS), "Consumer should successfully mark the candidate");

    var current = capturedCurrent.get();
    var candidate = capturedCandidate.get();

    assertNotNull(current);
    assertNotNull(candidate);

    boolean staleCasSucceeded = current.next().compareAndSet(candidate, candidate, false, false);

    assertFalse(staleCasSucceeded, "CAS expecting the old unmarked state must fail");

    boolean[] markHolder = new boolean[1];
    var reference = current.next().get(markHolder);

    assertSame(candidate, reference, "Reference should still point to the same candidate");

    assertTrue(markHolder[0], "Once marked, the link must remain marked");

    allowConsumerToReturn.countDown();
    consumer.join();

    assertEquals(42, result.get());
    assertNull(queue.dequeue());
  }

  @Test
  void dequeueRemainsCorrectWhileTailIsSeverelyLagging() throws Exception {
    var published = new CountDownLatch(1);
    var allowTailAdvance = new CountDownLatch(1);

    EnqueueProbe<Integer> enqueueProbe = new EnqueueProbe<>() {
      @Override
      public void afterOrdinaryLink(Node<Integer> observedTail, Node<Integer> node) {

        published.countDown();
        await(allowTailAdvance);
      }
    };

    var queue = new BasketsQueueSlackTail<Integer>(16, 16, enqueueProbe, new DequeueProbe<>() {
    });

    var producer = Thread.ofPlatform().start(() -> {
      queue.enqueue(1);
    });

    assertTrue(published.await(5, TimeUnit.SECONDS), "Producer should publish the first node before tail advances");

    /*
     * At this point:
     *
     * tail -> sentinel
     *
     * sentinel -> 1
     *
     * So tail is definitely stale.
     */

    assertEquals(1, queue.dequeue(), "Dequeue must succeed even though tail still points at the sentinel");

    assertNull(queue.dequeue(), "Queue should be logically empty after removing the only element");

    allowTailAdvance.countDown();
    producer.join();

    assertNull(queue.dequeue());
  }

  @Test
  void dequeueTraversesCorrectlyWithTailFarBehindPhysicalEnd() throws Exception {
    var firstPublished = new CountDownLatch(1);
    var allowFirstTailAdvance = new CountDownLatch(1);

    var helperBeforeRepair = new CountDownLatch(1);
    var allowHelperRepair = new CountDownLatch(1);

    var ordinaryLinks = new AtomicInteger();

    EnqueueProbe<Integer> enqueueProbe = new EnqueueProbe<>() {

      @Override
      public void afterOrdinaryLink(Node<Integer> observedTail, Node<Integer> node) {

        if (ordinaryLinks.incrementAndGet() == 1) {
          firstPublished.countDown();
          await(allowFirstTailAdvance);
        }
      }

      @Override
      public void beforeTailRepair(Node<Integer> observedTail, Node<Integer> candidate) {

        helperBeforeRepair.countDown();
        await(allowHelperRepair);
      }
    };

    var queue = new BasketsQueueSlackTail<Integer>(16, 16, enqueueProbe, new DequeueProbe<>() {
    });

    var p1 = Thread.ofPlatform().start(() -> queue.enqueue(1));

    assertTrue(firstPublished.await(5, TimeUnit.SECONDS), "First producer should publish before advancing tail");

    var p2 = Thread.ofPlatform().start(() -> queue.enqueue(2));

    assertTrue(helperBeforeRepair.await(5, TimeUnit.SECONDS), "Second producer should detect and try to repair lagging tail");

    /*
     * Right now we have at least:
     *
     * tail
     *  ↓
     *  S -> 1
     *
     * and p2 is frozen before fixing tail.
     *
     * Dequeue must derive correctness from the physical chain,
     * not from tail being current.
     */

    assertEquals(1, queue.dequeue());

    allowHelperRepair.countDown();
    allowFirstTailAdvance.countDown();

    p1.join();
    p2.join();

    assertEquals(2, queue.dequeue());
    assertNull(queue.dequeue());
  }

  @Test
  void physicalChainRemainsAcyclicUnderConcurrentBasketInsertion() throws Exception {
    int rounds = 500;
    int producersPerRound = 8;

    var queue = new BasketsQueueSlackTail<Integer>(16,     // MAX_RETRY_ATTEMPTS
        16,     // MAX_JUMPS
        new EnqueueProbe<>() {
        }, new DequeueProbe<>() {
    });

    var valueGenerator = new AtomicInteger();

    for (int round = 0; round < rounds; round++) {

      var ready = new CountDownLatch(producersPerRound);
      var start = new CountDownLatch(1);

      var threads = new ArrayList<Thread>();

      for (int i = 0; i < producersPerRound; i++) {
        int value = valueGenerator.incrementAndGet();

        threads.add(Thread.ofPlatform().start(() -> {
          ready.countDown();
          await(start);

          queue.enqueue(value);
        }));
      }

      assertTrue(ready.await(5, TimeUnit.SECONDS), "All producers should reach the starting gate");

      // Unleash the horde.
      start.countDown();

      for (var thread : threads) {
        thread.join();
      }

      assertPhysicalChainIsAcyclic(queue);
    }
  }

  @Test
  void physicalChainRemainsAcyclicWhenManyProducersCompeteForSameInsertionPoint() throws Exception {

    int producers = 8;

    var arrivedAtOrdinaryCas = new CountDownLatch(producers);
    var unleashCas = new CountDownLatch(1);

    var firstCollision = new AtomicBoolean(true);

    EnqueueProbe<Integer> probe = new EnqueueProbe<>() {

      @Override
      public void beforeOrdinaryLink(Node<Integer> observedTail, Node<Integer> node) {

        /*
         * Only coordinate the initial collision.
         *
         * Once producers retry elsewhere we must not keep
         * trapping them at this barrier.
         */
        if (firstCollision.get()) {
          arrivedAtOrdinaryCas.countDown();
          await(unleashCas);
        }
      }
    };

    var queue = new BasketsQueueSlackTail<Integer>(32, 16, probe, new DequeueProbe<>() {
    });

    var threads = new ArrayList<Thread>();

    for (int i = 0; i < producers; i++) {
      int value = i;

      threads.add(Thread.ofPlatform().start(() -> queue.enqueue(value)));
    }

    assertTrue(arrivedAtOrdinaryCas.await(5, TimeUnit.SECONDS), "Every producer should observe the initial insertion position");

    /*
     * Right now all eight producers have reasoned from roughly:
     *
     *          tail
     *            ↓
     *            S
     *            |
     *        S.next == null
     *
     * They are all about to attempt:
     *
     *     CAS(S.next, null, myNode)
     *
     * One wins.
     * Seven lose.
     *
     * Those seven are now eligible for our basket path.
     */

    firstCollision.set(false);
    unleashCas.countDown();

    for (var thread : threads) {
      thread.join();
    }

    assertPhysicalChainIsAcyclic(queue);
  }


  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 4, 8, 16, 32})
  void everySuccessfulEnqueuePublishesExactlyOneOccurrence(int maxRetryAttempts) throws Exception {

    int producers = 8;
    int valuesPerProducer = 1_000;
    int expectedValues = producers * valuesPerProducer;

    var queue = new BasketsQueueSlackTail<Integer>(1,      // deliberately tiny: force basket fallback aggressively
        16, new EnqueueProbe<>() {
    }, new DequeueProbe<>() {
    });

    var ready = new CountDownLatch(producers);
    var start = new CountDownLatch(1);

    var threads = new ArrayList<Thread>();

    for (int producer = 0; producer < producers; producer++) {
      int producerId = producer;

      threads.add(Thread.ofPlatform().start(() -> {
        ready.countDown();
        await(start);

        int base = producerId * valuesPerProducer;

        for (int i = 0; i < valuesPerProducer; i++) {
          queue.enqueue(base + i);
        }
      }));
    }

    assertTrue(ready.await(5, TimeUnit.SECONDS), "All producers should reach the starting gate");

    start.countDown();

    for (var thread : threads) {
      thread.join();
    }

    var seen = new HashSet<Integer>();

    Integer value;

    while ((value = queue.dequeue()) != null) {
      Integer finalValue = value;
      assertTrue(seen.add(value), () -> "Duplicate logical occurrence dequeued: " + finalValue);
    }

    assertEquals(expectedValues, seen.size(), "Every completed enqueue should publish exactly one value");

    for (int i = 0; i < expectedValues; i++) {
      int finalI = i;
      assertTrue(seen.contains(i), () -> "Missing enqueued value: " + finalI);
    }
  }

  @Test
  void dequeueUsesPhysicalEndToDetermineExhaustionEvenWhenTailIsMisleading() throws Exception {

    var queue = new BasketsQueueSlackTail<Integer>(16,     // MAX_RETRY_ATTEMPTS
        100,    // MAX_JUMPS: deliberately avoid head cleanup during setup
        new EnqueueProbe<>() {
        }, new DequeueProbe<>() {
    });

    queue.enqueue(1);
    queue.enqueue(2);
    queue.enqueue(3);

    /*
     * Mark all three nodes logically deleted.
     *
     * Because MAX_JUMPS is large, head should remain at the old sentinel
     * throughout this setup.
     */
    assertEquals(1, queue.dequeue());
    assertEquals(2, queue.dequeue());
    assertEquals(3, queue.dequeue());

    /*
     * Physical structure should now resemble:
     *
     * head
     *  ↓
     *  S -> 1 -> 2 -> 3 -> null
     *       X    X    X
     *
     * Logical queue is empty, but physical nodes remain.
     */

    var head = queue.headNode();

    boolean[] markHolder = new boolean[1];

    var one = head.next().get(markHolder);
    assertNotNull(one);
    assertTrue(markHolder[0], "1 should be logically deleted");

    var two = one.next().get(markHolder);
    assertNotNull(two);
    assertTrue(markHolder[0], "2 should be logically deleted");

    var three = two.next().get(markHolder);
    assertNotNull(three);
    assertTrue(markHolder[0], "3 should be logically deleted");

    assertNull(three.next().getReference(), "3 should be the physical end of the chain");

    /*
     * Now deliberately make tail misleading.
     *
     * Ideally expose a package-private test helper:
     *
     *     void setTailNodeForTest(Node<T> node)
     *
     * or manipulate the AtomicReference directly if your test already has
     * package-private access.
     *
     * Put tail somewhere inside the dead chain.
     */
    queue.setTailNodeForTest(one);

    assertSame(one, queue.tailNode());

    /*
     * This is the actual invariant under attack.
     *
     * dequeue() must not conclude anything from tail == some stale node.
     * It must traverse marks until candidate == null.
     */
    assertNull(queue.dequeue());

    /*
     * And after reaching physical end, your current implementation may
     * opportunistically advance head to the last dead node.
     */
    assertNull(queue.dequeue());
  }

  @Test
  void quiescentStructureRemainsSaneAfterHeavyMixedConcurrency() throws Exception {
    int producers = 8;
    int consumers = 8;
    int valuesPerProducer = 10_000;

    int expectedTotal = producers * valuesPerProducer;

    var queue = new BasketsQueueSlackTail<Integer>(4,      // MAX_RETRY_ATTEMPTS
        16,     // MAX_JUMPS
        new EnqueueProbe<>() {
        }, new DequeueProbe<>() {
    });

    var start = new CountDownLatch(1);

    var producedDone = new CountDownLatch(producers);

    var consumed = ConcurrentHashMap.<Integer>newKeySet();
    var duplicateDetected = new AtomicBoolean(false);

    var producerThreads = new ArrayList<Thread>();
    var consumerThreads = new ArrayList<Thread>();

    for (int producer = 0; producer < producers; producer++) {
      int producerId = producer;

      producerThreads.add(Thread.ofPlatform().start(() -> {
        await(start);

        int base = producerId * valuesPerProducer;

        for (int i = 0; i < valuesPerProducer; i++) {
          queue.enqueue(base + i);
        }

        producedDone.countDown();
      }));
    }

    for (int i = 0; i < consumers; i++) {
      consumerThreads.add(Thread.ofPlatform().start(() -> {
        await(start);

        while (true) {
          Integer value = queue.dequeue();

          if (value != null) {
            if (!consumed.add(value)) {
              duplicateDetected.set(true);
            }

            continue;
          }

          /*
           * Empty right now does not necessarily mean
           * producers are finished.
           */
          if (producedDone.getCount() == 0) {
            return;
          }

          Thread.onSpinWait();
        }
      }));
    }

    start.countDown();

    for (var thread : producerThreads) {
      thread.join();
    }

    for (var thread : consumerThreads) {
      thread.join();
    }

    /*
     * Consumers may all have observed an empty moment just after
     * producers finished while some logically live values remain.
     *
     * Drain anything left in the quiescent phase.
     */
    Integer value;

    while ((value = queue.dequeue()) != null) {
      if (!consumed.add(value)) {
        duplicateDetected.set(true);
      }
    }

    assertFalse(duplicateDetected.get(), "No value should ever be dequeued more than once");

    assertEquals(expectedTotal, consumed.size(), "Every produced value should eventually be accounted for");

    for (int i = 0; i < expectedTotal; i++) {
      int finalI = i;
      assertTrue(consumed.contains(i), () -> "Missing value: " + finalI);
    }

    /*
     * Whole-structure checks begin here.
     */
    assertPhysicalChainIsAcyclic(queue);

    assertTailIsAtPhysicalEnd(queue);

    assertAllReachableValueNodesAreLogicallyDeleted(queue);

    /*
     * Logical emptiness should now agree with repeated dequeue.
     */
    assertNull(queue.dequeue());
    assertNull(queue.dequeue());

    /*
     * Queue must still be reusable after all that contention
     * and cleanup.
     */
    queue.enqueue(Integer.MAX_VALUE);

    assertEquals(Integer.MAX_VALUE, queue.dequeue());

    assertNull(queue.dequeue());

    assertPhysicalChainIsAcyclic(queue);
    assertTailIsAtPhysicalEnd(queue);
  }

  static Stream<Arguments> basketQueuePolicies() {
    int[] retryAttempts = {0, 1, 2, 4, 16};
    int[] jumps = {0, 1, 2, 8, 32};

    return Arrays.stream(retryAttempts).boxed().flatMap(retry -> Arrays.stream(jumps).mapToObj(jump -> Arguments.of(retry, jump)));
  }

  @ParameterizedTest(name = "retries={0}, jumps={1}")
  @MethodSource("basketQueuePolicies")
  void correctnessDoesNotDependOnRetryOrCleanupPolicy(int maxRetryAttempts, int maxJumps) throws Exception {

    int producers = 8;
    int consumers = 8;
    int valuesPerProducer = 5_000;

    int expectedTotal = producers * valuesPerProducer;

    var queue = new BasketsQueueSlackTail<Integer>(maxRetryAttempts, maxJumps, new EnqueueProbe<>() {
    }, new DequeueProbe<>() {
    });

    var start = new CountDownLatch(1);
    var producersDone = new CountDownLatch(producers);

    var consumed = ConcurrentHashMap.<Integer>newKeySet();
    var duplicateDetected = new AtomicBoolean(false);

    var producerThreads = new ArrayList<Thread>();
    var consumerThreads = new ArrayList<Thread>();

    for (int producer = 0; producer < producers; producer++) {
      int producerId = producer;

      producerThreads.add(Thread.ofPlatform().start(() -> {
        await(start);

        int base = producerId * valuesPerProducer;

        for (int i = 0; i < valuesPerProducer; i++) {
          queue.enqueue(base + i);
        }

        producersDone.countDown();
      }));
    }

    for (int consumer = 0; consumer < consumers; consumer++) {
      consumerThreads.add(Thread.ofPlatform().start(() -> {
        await(start);

        while (true) {
          Integer value = queue.dequeue();

          if (value != null) {
            if (!consumed.add(value)) {
              duplicateDetected.set(true);
            }

            continue;
          }

          if (producersDone.getCount() == 0) {
            return;
          }

          Thread.onSpinWait();
        }
      }));
    }

    start.countDown();

    for (var thread : producerThreads) {
      thread.join();
    }

    for (var thread : consumerThreads) {
      thread.join();
    }

    /*
     * Final quiescent drain.
     */
    Integer value;

    while ((value = queue.dequeue()) != null) {
      if (!consumed.add(value)) {
        duplicateDetected.set(true);
      }
    }

    assertFalse(duplicateDetected.get(), "No logical value should be dequeued twice");

    assertEquals(expectedTotal, consumed.size(), () -> "Missing values with retries=" + maxRetryAttempts + ", jumps=" + maxJumps);

    for (int i = 0; i < expectedTotal; i++) {
      int expected = i;

      assertTrue(consumed.contains(expected), () -> "Missing value " + expected + " with retries=" + maxRetryAttempts + ", jumps=" + maxJumps);
    }

    assertPhysicalChainIsAcyclic(queue);
    assertAllReachableValueNodesAreLogicallyDeleted(queue);

    assertNull(queue.dequeue());

    /*
     * Reuse after the entire concurrent workload.
     */
    queue.enqueue(Integer.MAX_VALUE);

    assertEquals(Integer.MAX_VALUE, queue.dequeue());

    assertNull(queue.dequeue());

    assertPhysicalChainIsAcyclic(queue);
  }

  @Test
  void chaoticStressTest() throws Exception {
    int rounds = 50;

    var random = ThreadLocalRandom.current();

    for (int round = 0; round < rounds; round++) {
      int producers = random.nextInt(1, 17);
      int consumers = random.nextInt(1, 17);

      int maxRetryAttempts = random.nextInt(0, 17);
      int maxJumps = random.nextInt(0, 33);

      int valuesPerProducer = 2_000;
      int expectedTotal = producers * valuesPerProducer;

      var queue = new BasketsQueueSlackTail<Integer>(maxRetryAttempts, maxJumps, new EnqueueProbe<>() {
      }, new DequeueProbe<>() {
      });

      var start = new CountDownLatch(1);
      var producersDone = new CountDownLatch(producers);

      var consumed = ConcurrentHashMap.<Integer>newKeySet();
      var duplicateDetected = new AtomicBoolean(false);

      var threads = new ArrayList<Thread>();

      for (int p = 0; p < producers; p++) {
        int producerId = p;

        threads.add(Thread.ofPlatform().start(() -> {
          await(start);

          int base = producerId * valuesPerProducer;

          for (int i = 0; i < valuesPerProducer; i++) {
            queue.enqueue(base + i);

            if ((i & 63) == 0) {
              Thread.yield();
            }
          }

          producersDone.countDown();
        }));
      }

      for (int c = 0; c < consumers; c++) {
        threads.add(Thread.ofPlatform().start(() -> {
          await(start);

          while (true) {
            Integer value = queue.dequeue();

            if (value != null) {
              if (!consumed.add(value)) {
                duplicateDetected.set(true);
              }

              if ((value & 127) == 0) {
                Thread.yield();
              }

              continue;
            }

            if (producersDone.getCount() == 0) {
              return;
            }

            Thread.onSpinWait();
          }
        }));
      }

      start.countDown();

      for (var thread : threads) {
        thread.join();
      }

      Integer value;

      while ((value = queue.dequeue()) != null) {
        if (!consumed.add(value)) {
          duplicateDetected.set(true);
        }
      }

      assertFalse(duplicateDetected.get(), "Duplicate detected in round " + round);

      assertEquals(expectedTotal, consumed.size(), "Missing values in round " + round + " producers=" + producers + " consumers=" + consumers + " retries=" + maxRetryAttempts + " jumps=" + maxJumps);

      assertPhysicalChainIsAcyclic(queue);
      assertAllReachableValueNodesAreLogicallyDeleted(queue);

      queue.enqueue(Integer.MAX_VALUE);
      assertEquals(Integer.MAX_VALUE, queue.dequeue());
      assertNull(queue.dequeue());
    }
  }

  private static <T> void assertPhysicalChainIsAcyclic(BasketsQueueSlackTail<T> queue) {

    Set<Node<T>> visited = Collections.newSetFromMap(new IdentityHashMap<>());

    var current = queue.headNode();

    int traversed = 0;

    while (current != null) {

      int finalTraversed = traversed;
      assertTrue(visited.add(current), () -> "Cycle detected after traversing " + finalTraversed + " nodes");

      current = current.next().getReference();
      traversed++;

      assertTrue(traversed < 1_000_000, "Physical chain traversal appears not to terminate");
    }
  }

  private static <T> void assertAllReachableValueNodesAreLogicallyDeleted(BasketsQueueSlackTail<T> queue) {

    /*
     * There isn't actually a physical "before head" path we can walk,
     * because once head advances, those nodes are intentionally
     * unreachable from head.
     *
     * So the property we can inspect from the current head is:
     *
     * any nodes skipped by head advancement must already have been
     * logically deleted.
     *
     * The stronger proof of that came from our deterministic
     * head-cleanup tests.
     *
     * Here, in the quiescent state, simply verify that everything
     * still reachable is logically empty.
     */

    var current = queue.headNode();
    boolean[] markHolder = new boolean[1];

    while (true) {
      var candidate = current.next().get(markHolder);

      if (candidate == null) {
        return;
      }

      assertTrue(markHolder[0], "After full drain, every reachable value node should be marked dead");

      current = candidate;
    }
  }

  private static <T> void assertTailIsAtPhysicalEnd(BasketsQueueSlackTail<T> queue) {

    var tail = queue.tailNode();

    assertNotNull(tail);

    assertNull(tail.next().getReference(), "In a quiescent queue, tail should point at physical end");
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
