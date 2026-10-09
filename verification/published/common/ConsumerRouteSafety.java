package io.quotaflow.verification.published;

import java.lang.reflect.Method;
import java.util.Collection;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/** Reachability proof for immutable REST fixtures, never for arbitrary applications. */
public final class ConsumerRouteSafety {
    public static void verify(ConfigurableApplicationContext context) {
        if (!context.getBeansOfType(org.springframework.web.servlet.function.RouterFunction.class).isEmpty()
                || !context.getBeansOfType(org.springframework.web.reactive.function.server.RouterFunction.class).isEmpty()) {
            throw new AssertionError("Unreviewed functional route");
        }
        for (var mapping : context.getBeansOfType(org.springframework.web.servlet.function.support.RouterFunctionMapping.class).values())
            if (mapping.getRouterFunction() != null) throw new AssertionError("Unreviewed functional route");
        for (var mapping : context.getBeansOfType(org.springframework.web.reactive.function.server.support.RouterFunctionMapping.class).values())
            if (mapping.getRouterFunction() != null) throw new AssertionError("Unreviewed functional route");
        for (var mapping : context.getBeansOfType(org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping.class).values()) {
            for (var entry : mapping.getHandlerMethods().entrySet()) {
                verifyHandler(entry.getValue().getMethod(), entry.getKey().getProducesCondition().getProducibleMediaTypes());
            }
        }
        for (var mapping : context.getBeansOfType(org.springframework.web.reactive.result.method.annotation.RequestMappingHandlerMapping.class).values()) {
            for (var entry : mapping.getHandlerMethods().entrySet()) {
                verifyHandler(entry.getValue().getMethod(), entry.getKey().getProducesCondition().getProducibleMediaTypes());
            }
        }
        System.out.println("CONSUMER ROUTE SAFETY VERIFIED sseMappings=0 fragmentHandlers=0 functionalRoutes=0");
    }

    private static void verifyHandler(Method method, Collection<MediaType> produced) {
        String type = method.getGenericReturnType().getTypeName();
        if (produced.stream().anyMatch(media -> media.isCompatibleWith(MediaType.TEXT_EVENT_STREAM))
                || type.contains("SseEmitter") || type.contains("ResponseBodyEmitter")
                || type.contains("ServerSentEvent") || type.contains("FragmentsRendering")) {
            throw new AssertionError("Unreviewed SSE or fragment handler");
        }
    }

    public static void negativeControls() throws Exception {
        try (var context = new GenericApplicationContext()) {
            context.refresh();
            var mapping = new org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping();
            mapping.setApplicationContext(context);
            mapping.registerMapping(org.springframework.web.servlet.mvc.method.RequestMappingInfo.paths("/unsafe-sse")
                    .produces(MediaType.TEXT_EVENT_STREAM_VALUE).build(), new UnsafeSse(), UnsafeSse.class.getMethod("events"));
            context.getBeanFactory().registerSingleton("unsafeMapping", mapping);
            reject(context, "Unreviewed SSE or fragment handler");
        }
        try (var context = new GenericApplicationContext()) {
            context.getBeanFactory().registerSingleton("unsafeRoute", org.springframework.web.servlet.function.RouterFunctions.route(
                    org.springframework.web.servlet.function.RequestPredicates.headers(headers -> true),
                    request -> org.springframework.web.servlet.function.ServerResponse.ok().body("synthetic")));
            context.refresh();
            reject(context, "Unreviewed functional route");
        }
        try (var context = new GenericApplicationContext()) {
            context.getBeanFactory().registerSingleton("unsafeRoute", org.springframework.web.reactive.function.server.RouterFunctions.route(
                    org.springframework.web.reactive.function.server.RequestPredicates.headers(headers -> true),
                    request -> org.springframework.web.reactive.function.server.ServerResponse.ok().bodyValue("synthetic")));
            context.refresh();
            reject(context, "Unreviewed functional route");
        }
        System.out.println("CONSUMER ROUTE SAFETY NEGATIVE CONTROLS PASSED sse=blocked functionalMvc=blocked functionalReactive=blocked");
    }

    private static void reject(ConfigurableApplicationContext context, String expected) {
        try { verify(context); }
        catch (AssertionError failure) {
            if (!expected.equals(failure.getMessage())) throw failure;
            return;
        }
        throw new AssertionError("Unsafe route negative control was not rejected");
    }

    public static final class UnsafeSse {
        public SseEmitter events() { return new SseEmitter(); }
    }
}
