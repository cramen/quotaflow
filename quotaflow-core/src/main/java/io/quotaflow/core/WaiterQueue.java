package io.quotaflow.core;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded, per-policy waiter queue: strict priority (higher first), FIFO
 * within equal priority. All mutations happen under a lock that is never
 * held while a thread parks, and waiting is done with
 * {@link LockSupport#parkNanos} outside the lock, so virtual-thread callers
 * unmount instead of pinning a carrier and no {@code synchronized} appears on
 * the wait path.
 */
final class WaiterQueue {

    /**
     * One caller waiting for quota. Queue position is fixed at enqueue time
     * (priority and sequence never change); the wake time and the observed
     * configuration generation are updated in place after each lost retry,
     * which is equivalent to a re-enqueue because neither participates in
     * ordering.
     */
    static final class Waiter {
        final int priority;
        final long sequence;
        final long deadlineNanos;
        final Thread thread;
        volatile long wakeAtNanos;
        volatile long generation;

        Waiter(int priority, long sequence, long deadlineNanos, Thread thread, long wakeAtNanos,
                long generation) {
            this.priority = priority;
            this.sequence = sequence;
            this.deadlineNanos = deadlineNanos;
            this.thread = thread;
            this.wakeAtNanos = wakeAtNanos;
            this.generation = generation;
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final PriorityQueue<Waiter> waiters = new PriorityQueue<>(Comparator
            .<Waiter>comparingInt(waiter -> waiter.priority).reversed()
            .thenComparingLong(waiter -> waiter.sequence));
    private final int maxWaiters;

    WaiterQueue(int maxWaiters) {
        this.maxWaiters = maxWaiters;
    }

    /** Admits a new waiter, or returns {@code false} when full (backpressure). */
    boolean offer(Waiter waiter) {
        lock.lock();
        try {
            if (waiters.size() >= maxWaiters) {
                return false;
            }
            return waiters.add(waiter);
        } finally {
            lock.unlock();
        }
    }

    boolean isHead(Waiter waiter) {
        lock.lock();
        try {
            return waiters.peek() == waiter;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes the waiter wherever it is queued; wakes the new head when the
     * removed waiter was the head. Returns {@code true} when it was present.
     */
    boolean remove(Waiter waiter) {
        lock.lock();
        try {
            boolean wasHead = waiters.peek() == waiter;
            boolean removed = waiters.remove(waiter);
            if (removed && wasHead) {
                signalHeadLocked();
            }
            return removed;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Unparks the current head, if any. Used after head transitions and
     * configuration changes; unpark permits are coalesced by
     * {@link LockSupport}, so redundant signals are harmless and a signal
     * arriving before the head parks is not lost.
     */
    void signalHead() {
        lock.lock();
        try {
            signalHeadLocked();
        } finally {
            lock.unlock();
        }
    }

    int size() {
        lock.lock();
        try {
            return waiters.size();
        } finally {
            lock.unlock();
        }
    }

    private void signalHeadLocked() {
        Waiter head = waiters.peek();
        if (head != null) {
            LockSupport.unpark(head.thread);
        }
    }
}
