package org.dips.datastructure.queue;

import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;

public final class BasketsQueue<T> implements ConcurrentQueue<T> {

  private final AtomicReference<Node<T>> head;
  private final AtomicReference<Node<T>> tail;
  private final int MAX_RETRY_ATTEMPTS;

  private final EnqueueProbe<T> probe;

  public BasketsQueue(int maxRetryAttempts) {
    this(maxRetryAttempts, new EnqueueProbe<T>() {
    });
  }

  BasketsQueue(int maxRetryAttempts, EnqueueProbe<T> probe) {
    Node<T> node = new Node<>();
    head = new AtomicReference<>(node);
    tail = new AtomicReference<>(node);
    MAX_RETRY_ATTEMPTS = maxRetryAttempts;
    this.probe = probe;
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
    throw new UnsupportedOperationException("Not yet implemented ...");
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
}
