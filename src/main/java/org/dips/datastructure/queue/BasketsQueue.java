package org.dips.datastructure.queue;

import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;

public final class BasketsQueue<T> implements ConcurrentQueue<T> {

  private final AtomicReference<Node<T>> head;
  private final AtomicReference<Node<T>> tail;
  private final int MAX_RETRY_ATTEMPTS;

  public BasketsQueue(int maxRetryAttempts) {
    Node<T> node = new Node<>();
    head = new AtomicReference<>(node);
    tail = new AtomicReference<>(node);
    MAX_RETRY_ATTEMPTS = maxRetryAttempts;
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
        if (obsrvdTail.next.compareAndSet(null, node, false, false)) {
          tail.compareAndSet(obsrvdTail, node);
          return;
        }

        Node<T> current = obsrvdTail.next.get(markHolder);
        deleted = markHolder[0];
        int attempts = 0;

        while (!deleted && attempts < MAX_RETRY_ATTEMPTS) {

          node.next.set(current, false);

          if (obsrvdTail.next.compareAndSet(current, node, false, false)) {
            return;
          }

          current = obsrvdTail.next.get(markHolder);
          deleted = markHolder[0];
          attempts++;
        }

        node.next.set(null, false);
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
}
