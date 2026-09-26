package io.quotaflow.kotlin

import io.quotaflow.core.Decision
import io.quotaflow.core.DecisionListener
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Fan-out of finalized decisions to stream subscriptions. Called on the
 * limiter's decision path, so [onDecision] must never block: subscribers
 * receive events through a non-blocking `trySend` and slow collectors drop
 * events rather than delay decisions.
 */
internal class DecisionEventBus : DecisionListener {

    private val subscribers = CopyOnWriteArrayList<(DecisionEvent) -> Unit>()

    /** Registers a subscriber; the returned handle unregisters it. */
    fun subscribe(subscriber: (DecisionEvent) -> Unit): () -> Unit {
        subscribers.add(subscriber)
        return { subscribers.remove(subscriber) }
    }

    val subscriberCount: Int
        get() = subscribers.size

    override fun onDecision(decision: Decision, keyGroup: String) {
        val event = DecisionEvent(decision, keyGroup)
        for (subscriber in subscribers) {
            subscriber(event)
        }
    }
}
