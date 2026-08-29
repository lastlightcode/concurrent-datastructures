package org.dips.datastructure.queue;

import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;

public final class LockedQueue<T> implements ConcurrentQueue<T> {

  private final ReentrantLock lock = new ReentrantLock();

  private Node<T> head;
  private Node<T> tail;

  public LockedQueue() {
    Node<T> dummy = new Node<>(null);
    head = dummy;
    tail = dummy;
  }

  public void enqueue(T elem) {
    Node<T> node = new Node<>(elem);

    lock.lock();
    try {
      tail.next = node;
      tail = node;
    } finally {
      lock.unlock();
    }
  }

  public T dequeue() {
    lock.lock();

    try {
      Node<T> next = head.next;

      if (next == null) {
        return null;
      }

      T value = next.value;
      head = next;

      return value;
    } finally {
      lock.unlock();
    }
  }

  static final class Node<T> {
    final T value;
    Node<T> next;

    Node(T value) {
      this.value = value;
    }
  }
}
