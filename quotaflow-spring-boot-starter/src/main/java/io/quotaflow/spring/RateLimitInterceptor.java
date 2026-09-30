package io.quotaflow.spring;

import io.quotaflow.core.Decision;
import io.quotaflow.core.PolicySet;
import io.quotaflow.core.QuotaFlow;
import io.quotaflow.core.RateLimitContext;
import io.quotaflow.core.RateLimitPolicy;
import io.quotaflow.core.Scope;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.support.AopUtils;
import org.springframework.aop.ProxyMethodInvocation;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringValueResolver;

/**
 * AOP advice implementing {@link RateLimited}: resolves the limit key from the
 * compiled SpEL expression, maps annotation attributes onto a
 * {@link RateLimitContext} seeded from the invocation, and acquires through
 * the {@link QuotaFlow} facade.
 *
 * <p>Key expressions are parsed once per advised method. The evaluation
 * context exposes method arguments by parameter name, {@code #principal} /
 * {@code #authentication} when Spring Security is present, and
 * {@code #request} in servlet applications. The evaluated key seeds the
 * well-known context attribute matching the policy's scope; the security
 * principal additionally seeds the principal attribute unless the key already
 * did. Raw key material is never logged here, matching the core cardinality
 * rule.
 *
 * <p>Methods returning {@code Mono}/{@code Flux} (detected by return type,
 * only when Reactor is on the classpath) compose {@code tryAcquireAsync} /
 * {@code acquireAsync} into the returned publisher — the event loop is never
 * blocked, and the method body is not even assembled until the acquisition
 * allows it. Every other return type uses the blocking path: instant
 * {@code tryAcquire} when the wait timeout is zero, otherwise a throttled
 * {@code acquire} that parks (virtual-thread-friendly) until quota or timeout.
 * Rejections are raised as {@link RateLimitExceededException} — as a returned
 * failing publisher on the reactive path.
 */
public class RateLimitInterceptor implements MethodInterceptor, EmbeddedValueResolverAware, org.springframework.beans.factory.BeanClassLoaderAware {

    private boolean reactorPresent, securityPresent, servletPresent;

    @Override public void setBeanClassLoader(ClassLoader loader) {
        reactorPresent = ClassUtils.isPresent("reactor.core.publisher.Mono", loader);
        securityPresent = ClassUtils.isPresent("org.springframework.security.core.context.SecurityContextHolder", loader);
        servletPresent = ClassUtils.isPresent("org.springframework.web.context.request.RequestContextHolder", loader)
                && ClassUtils.isPresent("jakarta.servlet.http.HttpServletRequest", loader);
    }

    private final QuotaFlow quotaFlow;
    private final Supplier<PolicySet> policySets;
    private final SpelExpressionParser parser = new SpelExpressionParser();
    private final ParameterNameDiscoverer parameterNames = new DefaultParameterNameDiscoverer();
    private final ConcurrentHashMap<Method, CachedExpression> keyExpressions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Method, CachedExpression> weightExpressions = new ConcurrentHashMap<>();

    private record CachedExpression(String text, Expression expression) { }
    private record Identity(Object principal, Object authentication, String name) {
        static final Identity NONE = new Identity(null, null, null);
    }
    private StringValueResolver embeddedValueResolver;

    public RateLimitInterceptor(QuotaFlow quotaFlow, Supplier<PolicySet> policySets) {
        setBeanClassLoader(RateLimitInterceptor.class.getClassLoader());
        this.quotaFlow = Objects.requireNonNull(quotaFlow, "quotaFlow");
        this.policySets = Objects.requireNonNull(policySets, "policySets");
    }

    @Override
    public void setEmbeddedValueResolver(StringValueResolver resolver) {
        this.embeddedValueResolver = resolver;
    }

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        Method method = AopUtils.getMostSpecificMethod(
                invocation.getMethod(), AopUtils.getTargetClass(invocation.getThis()));
        RateLimited annotation = findAnnotation(method, invocation.getMethod());
        if (annotation == null) {
            return invocation.proceed();
        }
        if (reactorPresent && ReactorSupport.isReactive(method.getReturnType())) {
            if (!(invocation instanceof ProxyMethodInvocation proxy))
                throw new IllegalStateException("reactive limiting requires a cloneable Spring method invocation");
            var snapshot = (ProxyMethodInvocation) proxy.invocableClone(proxy.getArguments().clone());
            return ReactorSupport.invoke(this, snapshot, annotation, method);
        }
        RateLimitPolicy policy = policySets.get().policy(annotation.policy());
        Identity identity = securityPresent ? SecuritySupport.currentIdentity() : Identity.NONE;
        EvaluationContext evaluationContext = evaluationContext(invocation, method, identity, true);
        RateLimitContext context = limitingContext(annotation, policy, method, evaluationContext, identity);
        long weight = weight(annotation, method, evaluationContext);
        Duration waitTimeout = waitTimeout(annotation);
        Decision decision = waitTimeout.isZero()
                ? quotaFlow.tryAcquire(annotation.policy(), context, weight)
                : quotaFlow.acquire(annotation.policy(), context, weight, waitTimeout);
        if (!decision.isAllowed()) {
            throw new RateLimitExceededException(decision);
        }
        return invocation.proceed();
    }

    private static RateLimited findAnnotation(Method specificMethod, Method invocationMethod) {
        RateLimited annotation =
                AnnotatedElementUtils.findMergedAnnotation(specificMethod, RateLimited.class);
        return annotation != null
                ? annotation
                : AnnotatedElementUtils.findMergedAnnotation(invocationMethod, RateLimited.class);
    }

    /**
     * Builds the limiter context from the annotation and invocation. The SpEL
     * key seeds the well-known attribute matching the policy scope; the
     * security principal seeds the principal attribute when the key did not.
     */
    private RateLimitContext limitingContext(
            RateLimited annotation, RateLimitPolicy policy, Method method, EvaluationContext evaluationContext, Identity identity) {
        String expression = resolvePlaceholders(annotation.key());
        boolean explicit = expression != null && !expression.isBlank();
        String key = evaluateKey(expression, method, evaluationContext);
        if (key == null && !explicit && policy.scope() == Scope.USER) key = identity.name();
        RateLimitContext.Builder builder = RateLimitContext.builder();
        if (key == null && (explicit || policy.scope() != Scope.GLOBAL)) {
            if (annotation.onMissingKey() == OnMissingKey.REJECT || policy.defaultKey().isEmpty())
                throw new RateLimitExceededException(Decision.rejectedWithoutSchedule(policy.id(), policy.scope()));
            // An explicit missing key selecting a default must not silently become the principal.
        } else if (key != null) {
            switch (policy.scope()) {
                case TENANT -> builder.put(RateLimitContext.TENANT_ID, key);
                case USER -> builder.put(RateLimitContext.PRINCIPAL, key);
                case KEY -> builder.put(RateLimitContext.API_KEY, key);
                case GLOBAL -> { }
            }
        }
        if (identity.name() != null && policy.scope() != Scope.USER)
            builder.put(RateLimitContext.PRINCIPAL, identity.name());
        return builder.build();
    }

    private String evaluateKey(String keyExpression, Method method, EvaluationContext evaluationContext) {
        if (keyExpression == null || keyExpression.isBlank()) {
            return null;
        }
        Expression expression = cached(keyExpressions, method, keyExpression);
        Object value;
        try { value = expression.getValue(evaluationContext); }
        catch (org.springframework.expression.spel.SpelEvaluationException failure) {
            String code = failure.getMessageCode().name();
            if (code.equals("PROPERTY_OR_FIELD_NOT_READABLE_ON_NULL") || code.equals("METHOD_CALL_ON_NULL_OBJECT_NOT_ALLOWED")
                    || code.equals("CANNOT_INDEX_INTO_NULL_VALUE")) return null;
            throw failure;
        }
        if (value == null) {
            return null;
        }
        String key = String.valueOf(value);
        return key.isBlank() ? null : key;
    }

    private long weight(RateLimited annotation, Method method, EvaluationContext evaluationContext) {
        String text = requireText(resolvePlaceholders(annotation.weight()), "weight", method);
        long weight;
        if (text.startsWith("#")) {
            Expression expression =
                    cached(weightExpressions, method, text);
            Object value = expression.getValue(evaluationContext);
            if (!(value instanceof Number number)) {
                throw new IllegalStateException("weight expression '" + text + "' on method " + method
                        + " must evaluate to a number, got "
                        + (value == null ? "null" : value.getClass().getSimpleName()));
            }
            weight = number.longValue();
        } else {
            try {
                weight = Long.parseLong(text);
            } catch (NumberFormatException e) {
                throw new IllegalStateException("invalid weight '" + text + "' on method " + method
                        + "; expected a number or a SpEL expression starting with '#'", e);
            }
        }
        if (weight < 1) {
            throw new IllegalArgumentException(
                    "weight on method " + method + " must be >= 1, got " + weight);
        }
        return weight;
    }

    private Duration waitTimeout(RateLimited annotation) {
        String text = resolvePlaceholders(annotation.waitTimeout());
        Duration waitTimeout = DurationStyle.detectAndParse(text.trim());
        if (waitTimeout.isNegative()) {
            throw new IllegalArgumentException("waitTimeout must not be negative, got '" + text + "'");
        }
        return waitTimeout;
    }

    private Expression cached(ConcurrentHashMap<Method, CachedExpression> cache, Method method, String text) {
        return cache.compute(method, (key, old) -> old != null && old.text().equals(text)
                ? old : new CachedExpression(text, parser.parseExpression(text))).expression();
    }

    private EvaluationContext evaluationContext(MethodInvocation invocation, Method method, Identity identity, boolean servlet) {
        MethodBasedEvaluationContext context = new MethodBasedEvaluationContext(
                invocation.getThis() != null ? invocation.getThis() : method.getDeclaringClass(),
                method, invocation.getArguments(), parameterNames);
        context.setVariable("principal", identity.principal());
        context.setVariable("authentication", identity.authentication());
        if (servlet && servletPresent) {
            Object request = ServletSupport.currentRequest();
            if (request != null) context.setVariable("request", request);
        }
        return context;
    }

    private String resolvePlaceholders(String value) {
        return embeddedValueResolver != null ? embeddedValueResolver.resolveStringValue(value) : value;
    }

    private static String requireText(String value, String attribute, Method method) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    attribute + " on method " + method + " must not be blank");
        }
        return value.trim();
    }

    /** Spring Security access; loaded only when the security context holder is present. */
    private static final class SecuritySupport {
        static Identity currentIdentity() {
            return identity(org.springframework.security.core.context.SecurityContextHolder.getContext().getAuthentication());
        }
        static Identity identity(org.springframework.security.core.Authentication authentication) {
            if (authentication == null || !authentication.isAuthenticated()
                    || authentication instanceof org.springframework.security.authentication.AnonymousAuthenticationToken)
                return Identity.NONE;
            String name = authentication.getName();
            return new Identity(authentication.getPrincipal(), authentication,
                    name == null || name.isBlank() ? null : name);
        }
    }

    /** Reactive security linkage is isolated from servlet-only applications. */
    private static final class ReactiveSecuritySupport {
        static reactor.core.publisher.Mono<Object> context() {
            return org.springframework.security.core.context.ReactiveSecurityContextHolder.getContext()
                    .cast(Object.class).defaultIfEmpty(Identity.NONE);
        }
        static Identity identity(Object value) {
            return value instanceof org.springframework.security.core.context.SecurityContext context
                    ? SecuritySupport.identity(context.getAuthentication()) : Identity.NONE;
        }
    }

    /** Servlet request access; loaded only when the request context holder is present. */
    private static final class ServletSupport {

        static Object currentRequest() {
            org.springframework.web.context.request.RequestAttributes attributes =
                    org.springframework.web.context.request.RequestContextHolder.getRequestAttributes();
            if (attributes instanceof org.springframework.web.context.request.ServletRequestAttributes servlet) {
                return servlet.getRequest();
            }
            return null;
        }
    }

    /** Reactor composition; loaded only when {@code reactor.core.publisher.Mono} is present. */
    private static final class ReactorSupport {

        static boolean isReactive(Class<?> returnType) {
            return reactor.core.publisher.Mono.class.isAssignableFrom(returnType)
                    || reactor.core.publisher.Flux.class.isAssignableFrom(returnType);
        }

        private record Prepared(MethodInvocation invocation, RateLimitContext context, long weight) { }
        private static final class Subscription {
            final java.util.concurrent.atomic.AtomicBoolean active = new java.util.concurrent.atomic.AtomicBoolean(true);
            final java.util.concurrent.atomic.AtomicInteger invocation = new java.util.concurrent.atomic.AtomicInteger();
            void cancel() { active.set(false); invocation.compareAndSet(0, 2); }
            boolean begin() { return invocation.compareAndSet(0, 1); }
        }
        static Object invoke(RateLimitInterceptor interceptor, ProxyMethodInvocation snapshot,
                RateLimited annotation, Method method) {
            if (reactor.core.publisher.Mono.class.isAssignableFrom(method.getReturnType()))
                return reactor.core.publisher.Mono.defer(() -> {
                    var subscription = new Subscription();
                    return prepare(interceptor, snapshot, annotation, method, subscription)
                            .flatMap(invocation -> subscription.begin() ? proceedMono(invocation) : reactor.core.publisher.Mono.empty())
                            .doOnCancel(subscription::cancel);
                });
            return reactor.core.publisher.Flux.defer(() -> {
                var subscription = new Subscription();
                return prepare(interceptor, snapshot, annotation, method, subscription)
                        .flatMapMany(invocation -> subscription.begin() ? proceedFlux(invocation) : reactor.core.publisher.Flux.empty())
                        .doOnCancel(subscription::cancel);
            });
        }
        private static reactor.core.publisher.Mono<MethodInvocation> prepare(RateLimitInterceptor interceptor,
                ProxyMethodInvocation snapshot, RateLimited annotation, Method method, Subscription subscription) {
            long started = System.nanoTime();
            var policy = interceptor.policySets.get().policy(annotation.policy());
            Duration timeout = interceptor.waitTimeout(annotation);
            boolean positive = !timeout.isZero();
            Duration bound = positive ? timeout : Duration.ofSeconds(1);
            long deadline = started + bound.toNanos();
            Duration preparationBound = bound.compareTo(Duration.ofSeconds(1)) < 0 ? bound : Duration.ofSeconds(1);
            var authentication = interceptor.securityPresent ? ReactiveSecuritySupport.context() : reactor.core.publisher.Mono.just((Object)Identity.NONE);
            reactor.core.publisher.Mono<MethodInvocation> accepted = authentication.flatMap(value -> reactor.core.publisher.Mono.fromCompletionStage(
                    io.quotaflow.core.execution.BoundedExecution.shared().submit(() -> {
                        var invocation = snapshot.invocableClone(snapshot.getArguments().clone());
                        var identity = interceptor.securityPresent ? ReactiveSecuritySupport.identity(value) : Identity.NONE;
                        var evaluation = interceptor.evaluationContext(invocation, method, identity, false);
                        return new Prepared(invocation, interceptor.limitingContext(annotation, policy, method, evaluation, identity),
                                interceptor.weight(annotation, method, evaluation));
                    }, subscription.active::get)))
                    .timeout(preparationBound)
                    .flatMap(prepared -> {
                        long remaining = deadline - System.nanoTime();
                        if (remaining <= 0) return reactor.core.publisher.Mono.error(rejection(policy, positive, started, true, false));
                        var acquisition = positive
                                ? interceptor.quotaFlow.acquireAsync(policy.id(), prepared.context(), prepared.weight(), Duration.ofNanos(remaining))
                                : interceptor.quotaFlow.tryAcquireAsync(policy.id(), prepared.context(), prepared.weight());
                        return reactor.core.publisher.Mono.fromCompletionStage(acquisition).flatMap(decision -> decision.isAllowed()
                                ? reactor.core.publisher.Mono.just(prepared.invocation())
                                : reactor.core.publisher.Mono.error(new RateLimitExceededException(decision)));
                    })
                    ;
            // The default facade owns the deadline and its last known refill metadata.
            // An extra timer must not race it into an invented schedule-less timeout.
            if (!(interceptor.quotaFlow instanceof io.quotaflow.core.DefaultQuotaFlow)) accepted = accepted.timeout(bound);
            return accepted.onErrorMap(java.util.concurrent.TimeoutException.class, failure ->
                            rejection(policy, positive, started, System.nanoTime() - deadline >= 0, false))
                    .onErrorMap(java.util.concurrent.RejectedExecutionException.class, failure ->
                            rejection(policy, positive, started, false, true))
                    .doFinally(signal -> subscription.active.set(false));
        }
        private static RateLimitExceededException rejection(RateLimitPolicy policy, boolean positive, long started,
                boolean expired, boolean saturated) {
            Decision decision = Decision.rejectedWithoutSchedule(policy.id(), policy.scope());
            if (positive && expired) decision = decision.withThrottleRejection(io.quotaflow.core.ThrottleRejection.WAIT_TIMEOUT)
                    .withWait(Duration.ofNanos(Math.max(0, System.nanoTime() - started)));
            else if (positive && saturated && policy.reaction() == io.quotaflow.core.Reaction.THROTTLE)
                decision = decision.withThrottleRejection(io.quotaflow.core.ThrottleRejection.QUEUE_OVERFLOW);
            return new RateLimitExceededException(decision);
        }

        @SuppressWarnings("unchecked")
        private static reactor.core.publisher.Mono<Object> proceedMono(MethodInvocation invocation) {
            Object result;
            try {
                result = invocation.proceed();
            } catch (Throwable e) {
                return reactor.core.publisher.Mono.error(e);
            }
            return result != null
                    ? (reactor.core.publisher.Mono<Object>) result
                    : reactor.core.publisher.Mono.empty();
        }

        @SuppressWarnings("unchecked")
        private static reactor.core.publisher.Flux<Object> proceedFlux(MethodInvocation invocation) {
            Object result;
            try {
                result = invocation.proceed();
            } catch (Throwable e) {
                return reactor.core.publisher.Flux.error(e);
            }
            return result != null
                    ? (reactor.core.publisher.Flux<Object>) result
                    : reactor.core.publisher.Flux.empty();
        }
    }
}
