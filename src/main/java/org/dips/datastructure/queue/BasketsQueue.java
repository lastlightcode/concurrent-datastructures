package org.dips.datastructure.queue;

import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;

public final class BasketsQueue<T> implements ConcurrentQueue<T> {

  private static final int MAX_JUMPS = 16;

  private final AtomicReference<Node<T>> head;
  private final AtomicReference<Node<T>> tail;
  private final int MAX_RETRY_ATTEMPTS;

  private final EnqueueProbe<T> probe;
  private final DequeueProbe<T> dequeueProbe;

  public BasketsQueue(int maxRetryAttempts) {
    this(maxRetryAttempts, new EnqueueProbe<T>() {}, new DequeueProbe<T>() {});
  }

  BasketsQueue(int maxRetryAttempts, EnqueueProbe<T> probe,  DequeueProbe<T> dequeueProbe) {
    Node<T> node = new Node<>();

    head = new AtomicReference<>(node);
    tail = new AtomicReference<>(node);

    MAX_RETRY_ATTEMPTS = maxRetryAttempts;

    this.probe = probe;
    this.dequeueProbe = dequeueProbe;
  }

  public void enqueue(T elem) {
    Node<T> node = new Node<>(elem);
    boolean[] markHolder = new boolean[1];

    while (true) {
      Node<T> obsrvdTail = tail.get();
      Node<T> next = obsrvdTail.next.get(markHolder);
      boolean deleted = markHolder[0];

      if (obsrvdTail != tail.get()) {
        continue;
      }

      if (next == null && !deleted) {

        probe.beforeOrdinaryLink(obsrvdTail, node);

        if (obsrvdTail.next.compareAndSet(null, node, false, false)) {
          probe.afterOrdinaryLink(obsrvdTail, node);
          tail.compareAndSet(obsrvdTail, node);
          return;
        }

        Node<T> current = obsrvdTail.next.get(markHolder);
        deleted = markHolder[0];
        int attempts = 0;

        while (!deleted && attempts < MAX_RETRY_ATTEMPTS) {

          node.next.set(current, false);

          probe.beforeBasketCas(obsrvdTail, current, node);

          if (obsrvdTail.next.compareAndSet(current, node, false, false)) {
            return;
          }

          current = obsrvdTail.next.get(markHolder);
          deleted = markHolder[0];
          attempts++;
        }

        node.next.set(null, false);
        probe.afterBasketRetryExhausted(node);
        continue;
      }

      if (next != null) {
        var candidate = next;

        while (candidate.next.getReference() != null && tail.get() == obsrvdTail) {
          candidate = candidate.next.getReference();
        }

        tail.compareAndSet(obsrvdTail, candidate);
      }
    }
  }

  public T dequeue() {
    dequeue:
    while (true) { // dequeue loop
      var obsrvdHead = head.get();
      var obsrvdTail = tail.get();

      if (obsrvdHead != head.get()) {
        continue;
      }

      var next = obsrvdHead.next.getReference();

      if (obsrvdHead == obsrvdTail) {
        if (next == null) {
          return null; // the queue is empty
        }

        // head & tail are the same but next points to something so we have a lagging tail
        // catch up the lagging tail
        var candidate = next;

        while (candidate.next.getReference() != null && tail.get() == obsrvdTail) {
          candidate = candidate.next.getReference();
        }

        tail.compareAndSet(obsrvdTail, candidate);
      }

      // otherwise:
      // search from head for first unmarked link
      int jumps = 0;
      boolean[] markHolder = new boolean[1];
      var current = obsrvdHead;

      while (true) { // traverse chain of dead nodes
        var candidate = current.next.get(markHolder);
        var deleted = markHolder[0];

        if (candidate == null) {
          // reached physical end
          // everything traversed so far was dead
          if (current != obsrvdHead) {
            head.compareAndSet(obsrvdHead, current);
          }

          return null;
        }

        if (!deleted) {
          // candidate is the first live node
          // try to mark current.next
          if (current.next.compareAndSet(candidate, candidate, false, true)) {

            dequeueProbe.afterMarkCas(current, candidate);

            // clean when jumps reaches MAX_JUMPS
            if (jumps >= MAX_JUMPS) {
              head.compareAndSet(obsrvdHead, candidate);
            }

            return candidate.value;
          }

          continue dequeue;
        }

        // candidate is logically deleted
        jumps++;
        current = candidate;
      }
    }
  }

  Node<T> tailNode() {
    return tail.get();
  }

  static final class Node<T> {
    final T value;
    final AtomicMarkableReference<Node<T>> next;

    public Node() {
      this(null);
    }

    public Node(T elem) {
      this.value = elem;
      this.next = new AtomicMarkableReference<>(null, false);
    }
  }

  /**
   * Testing purposes only ...
   */
  interface EnqueueProbe<T> {

    default void afterOrdinaryLink(
        BasketsQueue.Node<T> observedTail,
        BasketsQueue.Node<T> node) {
    }

    default void beforeBasketCas(
        BasketsQueue.Node<T> observedTail,
        BasketsQueue.Node<T> current,
        BasketsQueue.Node<T> node) {
    }

    default void afterBasketCas(
        BasketsQueue.Node<T> observedTail,
        BasketsQueue.Node<T> node) {
    }

    default void beforeTailRepair(
        BasketsQueue.Node<T> observedTail) {
    }

    default void beforeOrdinaryLink(
        BasketsQueue.Node<T> observedTail,
        BasketsQueue.Node<T> node) {
    }

    default void afterBasketRetryExhausted(Node<T> node) {}
  }

  interface DequeueProbe<T> {

    default void beforeMarkCas(
        BasketsQueue.Node<T> current,
        BasketsQueue.Node<T> candidate) {
    }

    default void afterMarkCas(
        BasketsQueue.Node<T> current,
        BasketsQueue.Node<T> candidate) {
    }
  }
}
