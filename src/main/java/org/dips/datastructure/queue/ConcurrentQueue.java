package org.dips.datastructure.queue;

public sealed interface ConcurrentQueue<T> permits LockedQueue, LockFreeQueue, BasketsQueue, BasketsQueueSlackTail {

  void enqueue(T elem);

  T dequeue();
}
