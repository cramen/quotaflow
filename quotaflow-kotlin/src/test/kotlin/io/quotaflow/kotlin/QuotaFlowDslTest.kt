package io.quotaflow.kotlin

import io.quotaflow.config.ConfigurationParser
import io.quotaflow.core.Algorithm
import io.quotaflow.core.PolicyConfigurationException
import io.quotaflow.core.PolicySet
import io.quotaflow.core.Reaction
import io.quotaflow.core.Scope
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import org.junit.jupiter.api.Test

class QuotaFlowDslTest {

    @Test
    fun `dsl and properties model compile to an equivalent policy set`() {
        val dsl = quotaFlow {
            defaults {
                algorithm = Algorithm.GCRA
                reaction = Reaction.THROTTLE
                refillPeriod = 30.seconds
            }
            policy("global-cap") {
                limit(capacity = 10_000, refill = 10_000 every 1.minutes)
                scope = Scope.GLOBAL
            }
            policy("tenant-gold") {
                limit(1_000 every 1.minutes)
                scope = Scope.TENANT
                parent = "global-cap"
                algorithm = Algorithm.TOKEN_BUCKET
                reaction = Reaction.REJECT
                priority = 5
                keyResolver = "tenant-id"
                defaultKey = "anonymous"
            }
            policy("user-free") {
                limit(capacity = 100, refillAmount = 100)
                scope = Scope.USER
                parent = "tenant-gold"
            }
            policy("key-plan") {
                limitRef("plan")
                scope = Scope.KEY
                parent = "user-free"
            }
        }

        val properties = ConfigurationParser.parse(mapOf(
            "quotaflow.defaults.algorithm" to "gcra",
            "quotaflow.defaults.reaction" to "throttle",
            "quotaflow.defaults.refill-period" to "PT30S",
            "quotaflow.policies.global-cap.scope" to "global",
            "quotaflow.policies.global-cap.limit.capacity" to "10000",
            "quotaflow.policies.global-cap.limit.refill-amount" to "10000",
            "quotaflow.policies.global-cap.limit.refill-period" to "PT1M",
            "quotaflow.policies.tenant-gold.scope" to "tenant",
            "quotaflow.policies.tenant-gold.parent" to "global-cap",
            "quotaflow.policies.tenant-gold.algorithm" to "token-bucket",
            "quotaflow.policies.tenant-gold.reaction" to "reject",
            "quotaflow.policies.tenant-gold.priority" to "5",
            "quotaflow.policies.tenant-gold.key-resolver" to "tenant-id",
            "quotaflow.policies.tenant-gold.default-key" to "anonymous",
            "quotaflow.policies.tenant-gold.limit.capacity" to "1000",
            "quotaflow.policies.tenant-gold.limit.refill-amount" to "1000",
            "quotaflow.policies.tenant-gold.limit.refill-period" to "PT1M",
            "quotaflow.policies.user-free.scope" to "user",
            "quotaflow.policies.user-free.parent" to "tenant-gold",
            "quotaflow.policies.user-free.limit.capacity" to "100",
            "quotaflow.policies.user-free.limit.refill-amount" to "100",
            "quotaflow.policies.key-plan.scope" to "key",
            "quotaflow.policies.key-plan.parent" to "user-free",
            "quotaflow.policies.key-plan.limit-ref" to "plan",
        ))

        assertEquals(signature(properties.policySet()), signature(dsl))
    }

    @Test
    fun `cycle in parent references fails with the same error as the properties model`() {
        val dslError = assertThrows<PolicyConfigurationException> {
            quotaFlow {
                policy("a") { limit(1 every 1.seconds); scope = Scope.TENANT; parent = "b" }
                policy("b") { limit(1 every 1.seconds); scope = Scope.GLOBAL; parent = "a" }
            }
        }
        val propertiesError = assertThrows<PolicyConfigurationException> {
            ConfigurationParser.parse(mapOf(
                "quotaflow.policies.a.scope" to "tenant",
                "quotaflow.policies.a.parent" to "b",
                "quotaflow.policies.a.limit.capacity" to "1",
                "quotaflow.policies.a.limit.refill-amount" to "1",
                "quotaflow.policies.a.limit.refill-period" to "PT1S",
                "quotaflow.policies.b.scope" to "global",
                "quotaflow.policies.b.parent" to "a",
                "quotaflow.policies.b.limit.capacity" to "1",
                "quotaflow.policies.b.limit.refill-amount" to "1",
                "quotaflow.policies.b.limit.refill-period" to "PT1S",
            ))
        }
        assertEquals(propertiesError.message, dslError.message)
    }

    @Test
    fun `unknown parent fails with the same error as the properties model`() {
        val dslError = assertThrows<PolicyConfigurationException> {
            quotaFlow {
                policy("x") { limit(1 every 1.seconds); scope = Scope.GLOBAL; parent = "nope" }
            }
        }
        val propertiesError = assertThrows<PolicyConfigurationException> {
            ConfigurationParser.parse(mapOf(
                "quotaflow.policies.x.scope" to "global",
                "quotaflow.policies.x.parent" to "nope",
                "quotaflow.policies.x.limit.capacity" to "1",
                "quotaflow.policies.x.limit.refill-amount" to "1",
                "quotaflow.policies.x.limit.refill-period" to "PT1S",
            ))
        }
        assertEquals(propertiesError.message, dslError.message)
    }

    @Test
    fun `declaring both a static limit and a limit reference fails`() {
        val error = assertThrows<PolicyConfigurationException> {
            quotaFlow {
                policy("x") {
                    limit(1 every 1.seconds)
                    limitRef("plan")
                    scope = Scope.GLOBAL
                }
            }
        }
        assertEquals(
            "policy 'x' declares both a static limit and a dynamic limit reference 'plan';" +
                " exactly one limit source is allowed",
            error.message)
    }

    @Test
    fun `missing scope fails with an actionable error naming the policy`() {
        val error = assertThrows<PolicyConfigurationException> {
            quotaFlow {
                policy("x") { limit(1 every 1.seconds) }
            }
        }
        assertEquals("policy 'x' is missing required 'scope'", error.message)
    }

    @Test
    fun `limit without a refill period and no default fails with an actionable error`() {
        val error = assertThrows<PolicyConfigurationException> {
            quotaFlow {
                policy("x") { limit(capacity = 10, refillAmount = 10); scope = Scope.GLOBAL }
            }
        }
        assertEquals(
            "policy 'x' has no refill period: declare one via 'every'" +
                " or set 'refillPeriod' in the defaults block",
            error.message)
    }

    @Test
    fun `duplicate policy id fails like any other source`() {
        val error = assertThrows<PolicyConfigurationException> {
            quotaFlow {
                policy("dup") { limit(1 every 1.seconds); scope = Scope.GLOBAL }
                policy("dup") { limit(1 every 1.seconds); scope = Scope.GLOBAL }
            }
        }
        assertEquals("duplicate policy id 'dup'", error.message)
    }

    private fun signature(set: PolicySet): Map<String, List<Any?>> =
        set.policies().associate { policy ->
            policy.id() to listOf(
                policy.limit().map { listOf(it.capacity(), it.refillAmount(), it.refillPeriod()) }
                    .orElse(null),
                policy.limitRef().orElse(null),
                policy.algorithm(),
                policy.scope(),
                policy.reaction(),
                policy.priority(),
                policy.parentId().orElse(null),
                policy.keyResolverId().orElse(null),
                policy.defaultKey().orElse(null),
            )
        }
}
