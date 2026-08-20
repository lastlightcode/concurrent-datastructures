package org.dips.datastructure.queue;

public sealed interface ConcurrentQueue<T> permits LockedQueue, LockFreeQueue {

  void enqueue(T elem);

  T dequeue();
}
