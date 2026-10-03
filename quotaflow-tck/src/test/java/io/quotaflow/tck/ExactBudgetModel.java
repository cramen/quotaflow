package io.quotaflow.tck;

import java.math.BigInteger;

/** Independent reference: exact nanosecond credit with clock sampling separate from interval rounding. */
final class ExactBudgetModel {
    record Outcome(boolean allowed, long remaining, long retryMillis) { }
    private final BigInteger interval, maximum;
    private BigInteger credit;
    private long last;
    private final long capacity, clockQuantum;

    ExactBudgetModel(long capacity, long refill, long periodNanos, long clockQuantumNanos, long now) {
        this.capacity = capacity;
        clockQuantum = clockQuantumNanos;
        interval = ceiling(BigInteger.valueOf(periodNanos), BigInteger.valueOf(refill));
        maximum = BigInteger.valueOf(capacity).multiply(interval);
        credit = maximum;
        last = Math.floorDiv(now, clockQuantum) * clockQuantum;
    }

    Outcome acquire(long now, long weight) {
        now = Math.floorDiv(now, clockQuantum) * clockQuantum;
        if (now > last) {
            credit = credit.add(BigInteger.valueOf(now).subtract(BigInteger.valueOf(last))).min(maximum);
            last = now;
        }
        var cost = interval.multiply(BigInteger.valueOf(weight));
        boolean allowed = credit.compareTo(cost) >= 0;
        if (allowed) credit = credit.subtract(cost);
        long remaining = credit.divide(interval).longValueExact();
        long retry = allowed || weight > capacity ? 0 : ceiling(cost.subtract(credit), BigInteger.valueOf(1_000_000)).longValueExact();
        return new Outcome(allowed, remaining, retry);
    }

    private static BigInteger ceiling(BigInteger numerator, BigInteger denominator) {
        var quotient = numerator.divideAndRemainder(denominator);
        return quotient[0].add(quotient[1].signum() == 0 ? BigInteger.ZERO : BigInteger.ONE);
    }
}
