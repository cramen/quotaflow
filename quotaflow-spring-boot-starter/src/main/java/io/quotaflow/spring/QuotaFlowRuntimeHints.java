package io.quotaflow.spring;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;

/**
 * Spring AOT hints for the starter, registered through
 * {@code META-INF/spring/aot.factories}:
 * <ul>
 *   <li>the {@link RateLimited} annotation — the advisor's pointcut reads it
 *       reflectively when matching and proxying annotated user methods;</li>
 *   <li>the {@link QuotaFlowProperties} graph — {@code @ConfigurationProperties}
 *       binding in ahead-of-time processed applications.</li>
 * </ul>
 * The Lua script resources the store needs are covered by the store module's
 * own GraalVM reachability metadata.
 */
public final class QuotaFlowRuntimeHints implements RuntimeHintsRegistrar {

    @Override
    public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
        hints.reflection().registerType(RateLimited.class, MemberCategory.INVOKE_DECLARED_METHODS);
        hints.reflection().registerType(QuotaFlowProperties.class, MemberCategory.values());
        for (Class<?> nested : QuotaFlowProperties.class.getDeclaredClasses()) {
            hints.reflection().registerType(nested, MemberCategory.values());
        }
    }
}
