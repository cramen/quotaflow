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
public class RateLimitInterceptor implements MethodInterceptor, EmbeddedValueResolverAware {

    private static final boolean REACTOR_PRESENT =
            ClassUtils.isPresent("reactor.core.publisher.Mono", RateLimitInterceptor.class.getClassLoader());
    private static final boolean SECURITY_PRESENT = ClassUtils.isPresent(
            "org.springframework.security.core.context.SecurityContextHolder",
            RateLimitInterceptor.class.getClassLoader());
    private static final boolean SERVLET_PRESENT = ClassUtils.isPresent(
            "org.springframework.web.context.request.RequestContextHolder",
            RateLimitInterceptor.class.getClassLoader());

    private final QuotaFlow quotaFlow;
    private final Supplier<PolicySet> policySets;
    private final SpelExpressionParser parser = new SpelExpressionParser();
    private final ParameterNameDiscoverer parameterNames = new DefaultParameterNameDiscoverer();
    private final ConcurrentHashMap<Method, Expression> keyExpressions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Method, Expression> weightExpressions = new ConcurrentHashMap<>();

    private StringValueResolver embeddedValueResolver;

    public RateLimitInterceptor(QuotaFlow quotaFlow, Supplier<PolicySet> policySets) {
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
        RateLimitPolicy policy = policySets.get().policy(annotation.policy());
        boolean reactive = REACTOR_PRESENT && ReactorSupport.isReactive(method.getReturnType());
        EvaluationContext evaluationContext = evaluationContext(invocation, method);
        RateLimitContext context;
        try {
            context = limitingContext(annotation, policy, method, evaluationContext);
        } catch (RateLimitExceededException rejection) {
            if (reactive) {
                return ReactorSupport.failure(method.getReturnType(), rejection);
            }
            throw rejection;
        }
        long weight = weight(annotation, method, evaluationContext);
        Duration waitTimeout = waitTimeout(annotation);
        if (reactive) {
            return ReactorSupport.invoke(
                    invocation, quotaFlow, annotation.policy(), context, weight, waitTimeout,
                    method.getReturnType());
        }
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
            RateLimited annotation, RateLimitPolicy policy, Method method, EvaluationContext evaluationContext) {
        String key = evaluateKey(annotation, method, evaluationContext);
        RateLimitContext.Builder builder = RateLimitContext.builder();
        boolean principalSeeded = false;
        if (key == null) {
            // a global policy resolves to a single fixed bucket and never needs a key
            if (policy.scope() != Scope.GLOBAL
                    && annotation.onMissingKey() == OnMissingKey.REJECT) {
                throw new RateLimitExceededException(
                        Decision.rejectedWithoutSchedule(policy.id(), policy.scope()));
            }
            // USE_DEFAULT_KEY (or global scope): nothing seeded; the policy's
            // default key (if any) or the fixed global bucket applies
        } else {
            switch (policy.scope()) {
                case TENANT -> builder.put(RateLimitContext.TENANT_ID, key);
                case USER -> {
                    builder.put(RateLimitContext.PRINCIPAL, key);
                    principalSeeded = true;
                }
                case KEY -> builder.put(RateLimitContext.API_KEY, key);
                case GLOBAL -> {
                    // global policies resolve to a single fixed bucket
                }
            }
        }
        Object principal = currentPrincipal();
        if (principal != null && !principalSeeded) {
            builder.put(RateLimitContext.PRINCIPAL, SecuritySupport.principalName(principal));
        }
        return builder.build();
    }

    private String evaluateKey(RateLimited annotation, Method method, EvaluationContext evaluationContext) {
        String keyExpression = resolvePlaceholders(annotation.key());
        if (keyExpression == null || keyExpression.isBlank()) {
            return null;
        }
        Expression expression = keyExpressions.computeIfAbsent(method, m -> parser.parseExpression(keyExpression));
        Object value = expression.getValue(evaluationContext);
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
                    weightExpressions.computeIfAbsent(method, m -> parser.parseExpression(text));
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

    private EvaluationContext evaluationContext(MethodInvocation invocation, Method method) {
        MethodBasedEvaluationContext context = new MethodBasedEvaluationContext(
                invocation.getThis() != null ? invocation.getThis() : method.getDeclaringClass(),
                method, invocation.getArguments(), parameterNames);
        if (SECURITY_PRESENT) {
            Object principal = currentPrincipal();
            if (principal != null) {
                context.setVariable("principal", principal);
                context.setVariable("authentication", SecuritySupport.authentication());
            }
        }
        if (SERVLET_PRESENT) {
            Object request = ServletSupport.currentRequest();
            if (request != null) {
                context.setVariable("request", request);
            }
        }
        return context;
    }

    private Object currentPrincipal() {
        return SECURITY_PRESENT ? SecuritySupport.currentPrincipal() : null;
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

        static Object currentPrincipal() {
            org.springframework.security.core.Authentication authentication = authentication();
            return authentication != null && authentication.isAuthenticated()
                    ? authentication.getPrincipal()
                    : null;
        }

        static org.springframework.security.core.Authentication authentication() {
            return org.springframework.security.core.context.SecurityContextHolder
                    .getContext()
                    .getAuthentication();
        }

        static String principalName(Object principal) {
            org.springframework.security.core.Authentication authentication = authentication();
            return authentication != null ? authentication.getName() : String.valueOf(principal);
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

        static Object failure(Class<?> returnType, Throwable error) {
            return reactor.core.publisher.Mono.class.isAssignableFrom(returnType)
                    ? reactor.core.publisher.Mono.error(error)
                    : reactor.core.publisher.Flux.error(error);
        }

        static Object invoke(MethodInvocation invocation, QuotaFlow quotaFlow, String policyId,
                RateLimitContext context, long weight, Duration waitTimeout, Class<?> returnType) {
            reactor.core.publisher.Mono<Decision> decision = reactor.core.publisher.Mono.defer(() -> {
                CompletionStage<Decision> acquisition = waitTimeout.isZero()
                        ? quotaFlow.tryAcquireAsync(policyId, context, weight)
                        : quotaFlow.acquireAsync(policyId, context, weight, waitTimeout);
                return reactor.core.publisher.Mono.fromCompletionStage(acquisition);
            });
            if (reactor.core.publisher.Mono.class.isAssignableFrom(returnType)) {
                return decision.flatMap(d -> {
                    if (!d.isAllowed()) {
                        return reactor.core.publisher.Mono.error(new RateLimitExceededException(d));
                    }
                    return proceedMono(invocation);
                });
            }
            return decision.flatMapMany(d -> {
                if (!d.isAllowed()) {
                    return reactor.core.publisher.Flux.error(new RateLimitExceededException(d));
                }
                return proceedFlux(invocation);
            });
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
