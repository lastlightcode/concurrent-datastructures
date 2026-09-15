package org.dips.datastructure.queue;

import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.atomic.AtomicReference;

public final class BasketsQueueSlackTail<T> implements ConcurrentQueue<T> {

  private final int MAX_RETRY_ATTEMPTS;
  private final int MAX_JUMPS;

  private final AtomicReference<Node<T>> head;
  private final AtomicReference<Node<T>> tail;

  public BasketsQueueSlackTail(int maxRetryAttempts, int maxJumps) {
    Node<T> node = new Node<>();
    head = new AtomicReference<>(node);
    tail = new AtomicReference<>(node);
    MAX_RETRY_ATTEMPTS = maxRetryAttempts;
    MAX_JUMPS = maxJumps;
  }

  public void enqueue(T elem) {
    Node<T> node = new Node<>(elem);
    boolean[] markHolder = new boolean[1];

    retry:
    while (true) {

      Node<T> observedTail = tail.get();
      Node<T> candidate = observedTail;

      while (true) {

        Node<T> next = candidate.next.get(markHolder);
        boolean deleted = markHolder[0];

        if (observedTail != tail.get()) {
          continue retry;
        }

        if (next != null) {
          candidate = next;
          continue;
        }

        if (deleted) {
          continue retry;
        }

        /*
         * candidate is our observed physical end.
         */
        if (candidate.next.compareAndSet(null, node, false, false)) {

          /*
           * Don't update tail when we appended directly
           * after the tail we started from.
           *
           * Let it acquire some slack.
           */
          if (candidate != observedTail) {
            tail.compareAndSet(observedTail, node);
          }

          return;
        }

        /*
         * Someone beat us at exactly this insertion point.
         *
         * Now try joining their basket.
         */
        Node<T> current = candidate.next.get(markHolder);

        deleted = markHolder[0];
        int attempts = 0;

        while (!deleted && attempts < MAX_RETRY_ATTEMPTS) {

          node.next.set(current, false);

          if (candidate.next.compareAndSet(current, node, false, false)) {
            return;
          }

          current = candidate.next.get(markHolder);

          deleted = markHolder[0];
          attempts++;
        }

        node.next.set(null, false);
        continue retry;
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

  record Node<T>(T value, AtomicMarkableReference<Node<T>> next) {
    public Node() {
      this(null);
    }

    public Node(T elem) {
      this(elem, new AtomicMarkableReference<>(null, false));
    }
  }
}
