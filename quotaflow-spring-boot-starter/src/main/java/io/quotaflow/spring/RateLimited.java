package io.quotaflow.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Rate-limits invocations of the annotated method against the given policy.
 * The limit key is a Spring Expression Language (SpEL) expression evaluated
 * against the invocation: method arguments are available by parameter name
 * (Spring Boot applications compile with {@code -parameters} by default;
 * {@code #p0}/{@code #a0} positional forms always work), and when Spring
 * Security is present {@code #principal} and {@code #authentication} expose
 * the current authentication. In servlet applications the current
 * {@code HttpServletRequest} is available as {@code #request}.
 *
 * <p>The evaluated key feeds the well-known context attribute matching the
 * policy's scope (tenant id for tenant scope, principal for user scope, API
 * key for key scope); parent chain levels resolve from the security principal
 * and their own resolvers as usual. When the expression is blank or evaluates
 * to {@code null}/blank, {@link #onMissingKey()} decides: reject (the default)
 * or fall back to the policy's configured default key. A missing key never
 * means an unconditional allow.
 *
 * <p>Rejection surfaces as a {@link RateLimitExceededException} carrying the
 * decision; the auto-configured web handlers map it to HTTP 429 with
 * {@code Retry-After} and a problem+json body. Methods returning
 * {@code Mono}/{@code Flux} are limited and throttled without blocking the
 * event loop; all other return types use the blocking path (servlet workers
 * and virtual threads).
 *
 * <p>Standard Spring AOP caveats apply: the advice runs through the proxy, so
 * self-invocation and {@code final} methods/classes are not intercepted.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RateLimited {

    /** Id of the leaf policy to acquire against. */
    String policy();

    /**
     * SpEL expression producing the limit key (for example
     * {@code "#userId"} or {@code "#principal.name"}). Empty means no key is
     * seeded from the invocation — {@link #onMissingKey()} then applies unless
     * the security principal satisfies the policy's resolver.
     */
    String key() default "";

    /**
     * Tokens consumed per invocation: a number or a SpEL expression starting
     * with {@code '#'}. Property placeholders are resolved first.
     */
    String weight() default "1";

    /**
     * How long a throttled call may wait for quota before being rejected.
     * Zero (the default) means instant decisions only. Accepts ISO-8601
     * ({@code PT0.5S}) and simple ({@code 500ms}) duration forms; property
     * placeholders are resolved first.
     */
    String waitTimeout() default "0";

    /** Behavior when the key expression yields no key. */
    OnMissingKey onMissingKey() default OnMissingKey.REJECT;
}
