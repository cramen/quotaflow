package io.quotaflow.core;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.locks.ReentrantLock;

/** Bounded priority/FIFO continuations. No callback runs under the queue lock. */
final class WaiterQueue {
    static final class Waiter {
        final int priority;
        final long sequence;
        final Runnable signal;
        Waiter(int priority, long sequence, Runnable signal) {
            this.priority = priority; this.sequence = sequence; this.signal = signal;
        }
    }
    private final ReentrantLock lock = new ReentrantLock();
    private final PriorityQueue<Waiter> waiters = new PriorityQueue<>(Comparator
            .<Waiter>comparingInt(waiter -> waiter.priority).reversed().thenComparingLong(waiter -> waiter.sequence));
    private final int maxWaiters;
    WaiterQueue(int maxWaiters) { this.maxWaiters = maxWaiters; }
    boolean offer(Waiter waiter) {
        lock.lock();
        try { return waiters.size() < maxWaiters && waiters.add(waiter); }
        finally { lock.unlock(); }
    }
    boolean isHead(Waiter waiter) {
        lock.lock(); try { return waiters.peek() == waiter; } finally { lock.unlock(); }
    }
    boolean remove(Waiter waiter) {
        boolean removed; Waiter next;
        lock.lock();
        try { boolean head = waiters.peek() == waiter; removed = waiters.remove(waiter); next = head ? waiters.peek() : null; }
        finally { lock.unlock(); }
        if (next != null) next.signal.run();
        return removed;
    }
    void signalHead() {
        Waiter head;
        lock.lock(); try { head = waiters.peek(); } finally { lock.unlock(); }
        if (head != null) head.signal.run();
    }
    int size() { lock.lock(); try { return waiters.size(); } finally { lock.unlock(); } }
}
