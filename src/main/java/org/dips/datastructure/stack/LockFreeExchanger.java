package org.dips.datastructure.stack;

import org.dips.datastructure.stack.LockFreeExchanger.ExchangeResult.ExchangeFailed;
import org.dips.datastructure.stack.LockFreeExchanger.ExchangeResult.ExchangeSucceeded;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.dips.datastructure.stack.LockFreeExchanger.Operation.PUSH;
import static org.dips.datastructure.stack.LockFreeExchanger.State.BUSY;
import static org.dips.datastructure.stack.LockFreeExchanger.State.WAITING;

public class LockFreeExchanger<T> {

  private final AtomicReferenceArray<Exchange<T>> slots;
  private final long timeoutNanos;
  private final ThreadLocal<RangePolicy> rangePolicy;

  public LockFreeExchanger(int capacity, long timeoutNanos) {

    if (capacity <= 0) {
      throw new IllegalArgumentException("Capacity must be positive!");
    }

    if (timeoutNanos <= 0) {
      throw new IllegalArgumentException("Timeout must be positive");
    }

    slots = new AtomicReferenceArray<>(capacity);
    this.timeoutNanos = timeoutNanos;
    rangePolicy = ThreadLocal.withInitial(() -> new RangePolicy(capacity));
  }

  public ExchangeResult<T> visit(Operation operation, T value) {
    RangePolicy policy = rangePolicy.get();

    int slot = ThreadLocalRandom.current().nextInt(policy.range());

    Exchange<T> observed = slots.get(slot);

    if (observed == null) {
      return tryWait(slot, operation, value);
    }

    return tryMatch(slot, observed, operation, value);
  }

  private ExchangeResult<T> tryWait(int slot, Operation operation, T value) {
    Exchange<T> mine = new Exchange<>(operation, value, WAITING);

    if (!slots.compareAndSet(slot, null, mine)) {

      Exchange<T> observedAgain = slots.get(slot);

      if (observedAgain != null && observedAgain.state == WAITING && observedAgain.operation != operation) {
        return tryMatch(slot, observedAgain, operation, value);
      }

      return new ExchangeFailed<>(value);
    }

    long deadline = System.nanoTime() + timeoutNanos;

    while (System.nanoTime() - deadline < 0) {
      Exchange<T> observed = slots.get(slot);

      if (observed != null && observed.state == BUSY) {
        slots.compareAndSet(slot, observed, null);
        rangePolicy.get().recordSuccess();
        return new ExchangeSucceeded<>(observed.value);
      }

      Thread.onSpinWait();
    }

    if (slots.compareAndSet(slot, mine, null)) {
      rangePolicy.get().recordFailure();
      return new ExchangeFailed<>(value);
    }

    Exchange<T> observed = slots.get(slot);

    if (observed != null && observed.state == BUSY) {
      slots.compareAndSet(slot, observed, null);
      rangePolicy.get().recordSuccess();
      return new ExchangeSucceeded<>(observed.value);
    }

    return new ExchangeFailed<>(value);
  }

  private ExchangeResult<T> tryMatch(int slot, Exchange<T> observed, Operation operation, T value) {
    if (observed.state == BUSY) {
      return new ExchangeFailed<>(value);
    }

    if (observed.operation == operation) {
      return new ExchangeFailed<>(value);
    }

    T exchangedValue = operation == PUSH ? value : observed.value;

    Exchange<T> completed = new Exchange<>(operation, exchangedValue, BUSY);

    if (!slots.compareAndSet(slot, observed, completed)) {
      return new ExchangeFailed<>(value);
    }

    rangePolicy.get().recordSuccess();
    return new ExchangeSucceeded<>(exchangedValue);
  }

  enum Operation {
    POP,
    PUSH
  }

  enum State {
    WAITING,
    BUSY
  }

  record Exchange<T>(Operation operation, T value, State state) {
  }

  sealed interface ExchangeResult<T> permits ExchangeSucceeded, ExchangeFailed {

    record ExchangeSucceeded<T>(T value) implements ExchangeResult<T> {
    }

    record ExchangeFailed<T>(T value) implements ExchangeResult<T> {
    }
  }

  static final class RangePolicy {
    private final int maxRange;
    private int range = 1;

    RangePolicy(int maxRange) {
      this.maxRange = maxRange;
    }

    int range() {
      return range;
    }

    void recordSuccess() {
      if (range < maxRange) {
        range++;
      }
    }

    void recordFailure() {
      if (range > 1) {
        range--;
      }
    }
  }
}
