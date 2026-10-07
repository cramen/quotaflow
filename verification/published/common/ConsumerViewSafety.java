package io.quotaflow.verification.published;

import java.util.*;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.servlet.ViewResolver;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;
import org.springframework.web.servlet.view.*;

/** Runtime proof limited to these fixed REST fixtures; not an application-wide waiver. */
public final class ConsumerViewSafety {
    public static void negativeControl() throws Exception {
        Class<?> xslt;
        try { xslt=Class.forName("org.springframework.web.servlet.view.xslt.XsltView"); }
        catch (ClassNotFoundException removed) { return; }
        try (var unsafe=new org.springframework.context.support.GenericApplicationContext()) {
            unsafe.getBeanFactory().registerSingleton("unsafeView",xslt.getConstructor().newInstance()); unsafe.refresh();
            try { verify(unsafe); throw new IllegalStateException("XsltView negative control was not rejected"); }
            catch (AssertionError expected) {
                if (!expected.getMessage().equals("XsltView bean is present")) throw expected;
            }
        }
        System.out.println("CONSUMER VIEW SAFETY NEGATIVE CONTROL PASSED");
    }
    public static void verify(ConfigurableApplicationContext context) throws Exception {
        Class<?> xslt;
        try { xslt=Class.forName("org.springframework.web.servlet.view.xslt.XsltView"); }
        catch (ClassNotFoundException removed) {
            System.out.println("CONSUMER VIEW SAFETY VERIFIED xsltClass=absent"); return;
        }
        if (context.getBeanNamesForType(xslt,true,true).length!=0) throw new AssertionError("XsltView bean is present");
        Set<ViewResolver> seen=Collections.newSetFromMap(new IdentityHashMap<>());
        for (var resolver:context.getBeansOfType(ViewResolver.class).values()) verifyResolver(resolver,xslt,seen);
        for (var mapping:context.getBeansOfType(RequestMappingHandlerMapping.class).values()) {
            for (var entry:mapping.getHandlerMethods().entrySet())
                for (String pattern:entry.getKey().getPatternValues())
                    if (pattern.contains("**")) throw new AssertionError("Unreviewed wildcard view mapping");
        }
        System.out.println("CONSUMER VIEW SAFETY VERIFIED xsltBeans=0 wildcardViewMappings=0");
    }
    static void verifyResolver(ViewResolver resolver,Class<?> xslt,Set<ViewResolver> seen) throws Exception {
        if (!seen.add(resolver)) return;
        if (resolver.getClass()==ContentNegotiatingViewResolver.class) {
            var content=(ContentNegotiatingViewResolver)resolver;
            if (content.getDefaultViews()!=null && !content.getDefaultViews().isEmpty()) throw new AssertionError("Unreviewed default view");
            if (content.getViewResolvers()!=null) for (var child:content.getViewResolvers()) verifyResolver(child,xslt,seen);
        } else if (resolver.getClass()==ViewResolverComposite.class) {
            for (var child:((ViewResolverComposite)resolver).getViewResolvers()) verifyResolver(child,xslt,seen);
        } else if (resolver.getClass()==BeanNameViewResolver.class) {
            // All XsltView beans were independently rejected above.
        } else if (resolver.getClass()==InternalResourceViewResolver.class) {
            var method=UrlBasedViewResolver.class.getDeclaredMethod("getViewClass"); method.setAccessible(true);
            Object selected=method.invoke(resolver);
            if (!(selected instanceof Class<?> type) || xslt.isAssignableFrom(type)) throw new AssertionError("Unsafe view class");
        } else throw new AssertionError("Unreviewed view resolver: "+resolver.getClass().getName());
    }
}
