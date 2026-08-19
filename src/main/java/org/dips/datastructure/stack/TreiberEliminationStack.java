package org.dips.datastructure.stack;

import org.dips.datastructure.stack.LockFreeExchanger.ExchangeResult;
import org.dips.datastructure.stack.LockFreeExchanger.ExchangeResult.ExchangeSucceeded;

import java.util.EmptyStackException;
import java.util.concurrent.atomic.AtomicReference;

import static org.dips.datastructure.stack.LockFreeExchanger.Operation.POP;
import static org.dips.datastructure.stack.LockFreeExchanger.Operation.PUSH;

public class TreiberEliminationStack<E> {

  private final AtomicReference<Node<E>> head = new AtomicReference<>();
  private final LockFreeExchanger<E> lockFreeExchanger = new LockFreeExchanger<>(32, 10_000);

  public void push(E elem) {
    var newHead = new Node<>(elem);

    while (true) {
      Node<E> top = head.get();
      newHead.next = top;

      if (head.compareAndSet(top, newHead)) {
        return;
      }

      var exchangeResult = lockFreeExchanger.visit(PUSH, elem);

      if (exchangeResult instanceof ExchangeSucceeded<E>) {
        return;
      }
    }
  }

  public E pop() {
    while (true) {
      Node<E> observedHead = head.get();

      if (observedHead == null) {
        var exchangeResult = lockFreeExchanger.visit(POP, null);

        if (exchangeResult instanceof ExchangeSucceeded<E>(E value)) {
          return value;
        }

        throw new EmptyStackException();
      }

      Node<E> newHead = observedHead.next;

      if (head.compareAndSet(observedHead, newHead)) {
        return observedHead.item;
      }

      ExchangeResult<E> result = lockFreeExchanger.visit(POP, null);

      if (result instanceof ExchangeSucceeded<E>(E value)) {
        return value;
      }
    }
  }

  private static final class Node<E> {
    public final E item;
    private Node<E> next;

    public Node(E item) {
      this.item = item;
    }
  }
}
