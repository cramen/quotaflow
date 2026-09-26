package io.quotaflow.kotlin

import io.quotaflow.core.Algorithm
import io.quotaflow.core.Limit
import io.quotaflow.core.PolicyConfigurationException
import io.quotaflow.core.PolicySet
import io.quotaflow.core.RateLimitPolicy
import io.quotaflow.core.Reaction
import io.quotaflow.core.Scope
import kotlin.time.Duration
import kotlin.time.toJavaDuration

/**
 * Type-safe configuration DSL compiling into a [PolicySet] through the same
 * [PolicySet.compile] pipeline as every other configuration source — same
 * validation, same error messages, zero new rules.
 *
 * ```kotlin
 * val set = quotaFlow {
 *     defaults { algorithm = Algorithm.TOKEN_BUCKET; reaction = Reaction.REJECT; refillPeriod = 1.seconds }
 *     policy("llm-provider") { limit(capacity = 10_000, refill = 10_000 every 1.minutes); scope = Scope.GLOBAL }
 *     policy("tenant-gold") { limit(1_000 every 1.minutes); scope = Scope.TENANT; parent = "llm-provider" }
 * }
 * ```
 */
public fun quotaFlow(configure: PolicySetDefinition.() -> Unit): PolicySet {
    val definition = PolicySetDefinition().apply(configure)
    return definition.compile()
}

/** Refill schedule of a limit: [amount] tokens every [period]. */
public data class Refill(
    public val amount: Long,
    public val period: Duration,
)

/** Refill schedule expression: `1_000 every 1.minutes`. */
public infix fun Long.every(period: Duration): Refill = Refill(this, period)

/** Refill schedule expression: `1_000 every 1.minutes`. */
public infix fun Int.every(period: Duration): Refill = Refill(this.toLong(), period)

@DslMarker
private annotation class QuotaFlowDsl

/** Root of the [quotaFlow] DSL: global defaults plus the declared policies. */
@QuotaFlowDsl
public class PolicySetDefinition internal constructor() {
    private val defaults = DefaultsDefinition()
    private val policies = mutableListOf<RateLimitPolicy>()

    /** Global defaults applied to policies that do not override them. */
    public fun defaults(configure: DefaultsDefinition.() -> Unit) {
        defaults.configure()
    }

    /** Declares one policy. */
    public fun policy(id: String, configure: PolicyDefinition.() -> Unit) {
        policies += PolicyDefinition(id).apply(configure).compile(defaults)
    }

    internal fun compile(): PolicySet = PolicySet.compile(policies)
}

/** Global defaults; unset values fall back to the policy model's own defaults. */
@QuotaFlowDsl
public class DefaultsDefinition internal constructor() {
    public var algorithm: Algorithm = Algorithm.TOKEN_BUCKET
    public var reaction: Reaction = Reaction.REJECT

    /** Refill period for limits declared without an explicit period. */
    public var refillPeriod: Duration? = null
}

/** One policy declaration inside the [quotaFlow] DSL. */
@QuotaFlowDsl
public class PolicyDefinition internal constructor(private val id: String) {
    /** Breadth of this policy level (required). */
    public var scope: Scope? = null

    /** Overrides the default algorithm. */
    public var algorithm: Algorithm? = null

    /** Overrides the default reaction. */
    public var reaction: Reaction? = null

    /** Default waiter queue priority for throttle-mode acquisitions. */
    public var priority: Int = 0

    /** Id of the parent policy (strictly broader scope). */
    public var parent: String? = null

    /** Id of a named key resolver registered on the facade. */
    public var keyResolver: String? = null

    /** Fixed fallback key used when key resolution yields nothing. */
    public var defaultKey: String? = null

    private var limitRef: String? = null
    private var capacity: Long? = null
    private var refillAmount: Long? = null
    private var refillPeriod: Duration? = null

    /** Static limit with equal capacity and refill: `limit(1_000 every 1.minutes)`. */
    public fun limit(refill: Refill) {
        limit(capacity = refill.amount, refill = refill)
    }

    /** Static limit: up to [capacity] tokens, refilled by [refill]. */
    public fun limit(capacity: Long, refill: Refill) {
        this.capacity = capacity
        this.refillAmount = refill.amount
        this.refillPeriod = refill.period
    }

    /**
     * Static limit whose refill period falls back to
     * [DefaultsDefinition.refillPeriod] when [refillPeriod] is not given.
     */
    public fun limit(capacity: Long, refillAmount: Long, refillPeriod: Duration? = null) {
        this.capacity = capacity
        this.refillAmount = refillAmount
        this.refillPeriod = refillPeriod
    }

    /** Dynamic limit reference resolved at decision time; mutually exclusive with [limit]. */
    public fun limitRef(ref: String) {
        this.limitRef = ref
    }

    internal fun compile(defaults: DefaultsDefinition): RateLimitPolicy {
        val builder = RateLimitPolicy.builder(id)
            .algorithm(algorithm ?: defaults.algorithm)
            .reaction(reaction ?: defaults.reaction)
            .priority(priority)
        builder.scope(
            scope ?: throw PolicyConfigurationException("policy '$id' is missing required 'scope'"))
        parent?.let(builder::parentId)
        keyResolver?.let(builder::keyResolverId)
        defaultKey?.let(builder::defaultKey)
        limitRef?.let(builder::limitRef)
        val declaredCapacity = capacity
        if (declaredCapacity != null) {
            val period = refillPeriod ?: defaults.refillPeriod
                ?: throw PolicyConfigurationException(
                    "policy '$id' has no refill period: declare one via 'every'" +
                        " or set 'refillPeriod' in the defaults block")
            builder.limit(Limit(declaredCapacity, refillAmount!!, period.toJavaDuration()))
        }
        return builder.build()
    }
}
