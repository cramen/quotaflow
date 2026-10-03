package io.quotaflow.tck;

import java.nio.file.Path;
import java.util.List;
import jdk.jfr.consumer.RecordingFile;

/** Conservative attribution: only explicit JVM initialization evidence is exempt in the cold phase. */
final class PinningEvidence {
    public static void main(String[] args) throws Exception {
        var summary = read(Path.of(args[0]));
        System.out.println("initialization=" + summary.initialization() + ", library=" + summary.library() + ", unknown=" + summary.unattributed());
        summary.events().stream().filter(event -> !event.startsWith("JVM_INITIALIZATION")).forEach(System.out::println);
    }
    enum Attribution { JVM_INITIALIZATION, LIBRARY_BLOCKING, UNATTRIBUTED }
    record Summary(int total, int initialization, int library, int unattributed, List<String> events) { }
    static Summary read(Path recording) throws Exception {
        int initialization = 0, library = 0, unattributed = 0;
        var events = new java.util.ArrayList<String>();
        for (var event : RecordingFile.readAllEvents(recording)) {
            if (!event.getEventType().getName().equals("jdk.VirtualThreadPinned")) continue;
            var trace = event.getStackTrace();
            var frames = trace == null ? List.<String>of() : trace.getFrames().stream()
                    .map(frame -> frame.getMethod().getType().getName() + "#" + frame.getMethod().getName()).toList();
            String reason = event.hasField("pinnedReason") ? event.getString("pinnedReason") : "";
            Attribution attribution = classify(reason, frames);
            if (attribution != Attribution.JVM_INITIALIZATION && trace != null && !trace.getFrames().isEmpty()
                    && event.hasField("blockingOperation") && "Contended monitor enter".equals(event.getString("blockingOperation"))
                    && ("Freeze or preempt failed (2)".equals(reason) || (reason != null && reason.startsWith("VM call to ") && reason.endsWith(".<clinit> on stack")))
                    && InitializationInstruction.provesResolution(trace.getFrames().get(0))) {
                attribution = Attribution.JVM_INITIALIZATION;
            }
            var top = trace == null || trace.getFrames().isEmpty() ? null : trace.getFrames().get(0);
            events.add(attribution + " bci=" + (top == null ? -1 : top.getBytecodeIndex()) + " " + event);
            switch (attribution) {
                case JVM_INITIALIZATION -> initialization++;
                case LIBRARY_BLOCKING -> library++;
                case UNATTRIBUTED -> unattributed++;
            }
        }
        return new Summary(events.size(), initialization, library, unattributed, List.copyOf(events));
    }
    static Attribution classify(String reason, List<String> frames) {
        if (frames.isEmpty()) return Attribution.UNATTRIBUTED;
        // A class-loader frame elsewhere in a stack does not excuse blocking above it.
        boolean loader = false;
        boolean linkage = false;
        boolean lambdaCache = frames.get(0).equals("java.lang.invoke.MethodTypeForm#setCachedLambdaForm");
        for (String frame : frames) {
            if (!(frame.startsWith("java.") || frame.startsWith("jdk.") || frame.startsWith("sun."))) break;
            if (lambdaCache && (frame.startsWith("java.lang.invoke.MethodHandleNatives#linkCallSite")
                    || frame.equals("java.lang.invoke.DirectMethodHandle#preparedLambdaForm"))) linkage = true;
            if (frame.startsWith("jdk.internal.loader.BuiltinClassLoader#loadClass")
                    || frame.startsWith("java.lang.ClassLoader#loadClass")
                    || frame.startsWith("java.lang.ClassLoader#getClassLoadingLock")
                    || frame.startsWith("java.lang.ClassLoader#defineClass")) { loader = true; break; }
        }
        boolean initializer = reason != null && reason.matches("Waited for initialization of [\\w.$/]+ by another thread");
        boolean ownInitializer = frames.stream().anyMatch(frame -> owned(frame) && frame.endsWith("#<clinit>"));
        if (loader || (initializer && !ownInitializer) || (linkage && "Native or VM frame on stack".equals(reason))) return Attribution.JVM_INITIALIZATION;
        return frames.stream().anyMatch(PinningEvidence::owned) ? Attribution.LIBRARY_BLOCKING : Attribution.UNATTRIBUTED;
    }
    private static boolean owned(String frame) {
        return frame.startsWith("io.quotaflow.core.") || frame.startsWith("io.quotaflow.store.") || frame.startsWith("io.quotaflow.fallback.");
    }
}
