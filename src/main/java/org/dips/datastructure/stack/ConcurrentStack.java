package org.dips.datastructure.stack;

import java.util.concurrent.atomic.AtomicReference;

public class ConcurrentStack<E> {

  private AtomicReference<Node<E>> head = new AtomicReference<>();

  public void push(E item) {
    Node<E> oldHead;
    Node<E> newHead = new Node<>(item);

    do {
      oldHead = head.get();
      newHead.next = oldHead;
    } while (!head.compareAndSet(oldHead, newHead));
  }

  public E pop() {
    Node<E> oldHead;
    Node<E> newHead;

    do {
      oldHead = head.get();
      if (oldHead == null) {
        return null;
      }
      newHead = oldHead.next;
    } while(!head.compareAndSet(oldHead, newHead));

    return oldHead.item;
  }

  private static class Node<E> {
    public final E item;
    private Node<E> next;

    public Node(E item) {
      this.item = item;
    }
  }
}
