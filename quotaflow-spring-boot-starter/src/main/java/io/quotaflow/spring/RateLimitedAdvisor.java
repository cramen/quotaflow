package io.quotaflow.spring;

import java.io.Serial;
import java.util.Objects;
import org.springframework.aop.Pointcut;
import org.springframework.aop.support.AbstractPointcutAdvisor;
import org.springframework.aop.support.annotation.AnnotationMatchingPointcut;

/**
 * Advisor applying {@link RateLimitInterceptor} to every method annotated with
 * {@link RateLimited}. Replace it with an own bean of this type to customize
 * the pointcut (for example to add class-level matching).
 */
public class RateLimitedAdvisor extends AbstractPointcutAdvisor {

    @Serial
    private static final long serialVersionUID = 1L;

    private final transient org.aopalliance.intercept.MethodInterceptor interceptor;
    private final transient Pointcut pointcut =
            new AnnotationMatchingPointcut(null, RateLimited.class, true);

    public RateLimitedAdvisor(RateLimitInterceptor interceptor) {
        this.interceptor = Objects.requireNonNull(interceptor, "interceptor");
    }

    private RateLimitedAdvisor(java.util.function.Supplier<RateLimitInterceptor> interceptor) {
        Objects.requireNonNull(interceptor, "interceptor");
        this.interceptor = invocation -> interceptor.get().invoke(invocation);
    }

    /** Defers runtime dependencies while Spring inspects pointcuts during AOT processing. */
    static RateLimitedAdvisor deferred(java.util.function.Supplier<RateLimitInterceptor> interceptor) {
        return new RateLimitedAdvisor(interceptor);
    }

    @Override
    public org.aopalliance.intercept.MethodInterceptor getAdvice() {
        return interceptor;
    }

    @Override
    public Pointcut getPointcut() {
        return pointcut;
    }
}
