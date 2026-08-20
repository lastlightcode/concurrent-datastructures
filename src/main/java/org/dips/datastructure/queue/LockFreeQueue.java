package org.dips.datastructure.queue;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

public final class LockFreeQueue<T> implements ConcurrentQueue<T> {

  private final AtomicReference<Node<T>> head;
  private final AtomicReference<Node<T>> tail;


  public LockFreeQueue() {
    Node<T> node = new Node<>();
    head = new AtomicReference<>(node);
    tail = new AtomicReference<>(node);
  }

  public void enqueue(T elem) {
    Node<T> node = new Node<>(elem);

    while (true) {
      Node<T> observdTail = tail.get();
      Node<T> next = observdTail.next.get();

      if (observdTail == tail.get()) {
        if (next == null) {
          if (observdTail.next.compareAndSet(null, node)) {
            tail.compareAndSet(observdTail, node);
            return;
          }
        } else {
          tail.compareAndSet(observdTail, next);
        }
      }
    }
  }

  public T dequeue() {
    while (true) {
      Node<T> obsrvdHead = head.get();
      Node<T> obsrvdTail = tail.get();
      Node<T> obsrvdHdNxt = obsrvdHead.next.get();

      if (obsrvdHead == head.get()) {
        if (obsrvdHead == obsrvdTail) {
          if (obsrvdHdNxt == null) {
            throw new NoSuchElementException();
          }
          tail.compareAndSet(obsrvdTail, obsrvdHdNxt);
        } else {
          T value = obsrvdHdNxt.value;
          if (head.compareAndSet(obsrvdHead, obsrvdHdNxt)) {
            return value;
          }
        }
      }
    }
  }

  static class Node<T> {
    T value;
    AtomicReference<Node<T>> next;

    public Node() {
      this(null);
    }

    public Node(T elem) {
      value = elem;
      next = new AtomicReference<>(null);
    }
  }
}
