package io.quotaflow.tck;
import java.nio.file.*;
import java.time.Duration;
import java.util.List;
import jdk.jfr.*;
import jdk.jfr.consumer.RecordingFile;
import static org.junit.jupiter.api.Assertions.*;
class PinningControlTest {
    @org.junit.jupiter.api.Test
    void recordingDetectsTheKnownMonitorPinOnJdk21() throws Exception {
        Path path = Path.of("build/reports/virtual-threads", "jdk" + Runtime.version().feature(), "negative-control.jfr");
        Files.createDirectories(path.getParent());
        try (var recording = new Recording()) {
            recording.enable("jdk.VirtualThreadPinned").withThreshold(Duration.ZERO).withStackTrace();
            recording.start();
            Thread.ofVirtual().start(io.quotaflow.core.verification.KnownMonitorPin::park).join();
            recording.stop(); recording.dump(path);
        }
        var pins = RecordingFile.readAllEvents(path).stream().filter(event -> event.getEventType().getName().equals("jdk.VirtualThreadPinned"))
                .filter(event -> event.getStackTrace() != null && event.getStackTrace().getFrames().stream()
                        .anyMatch(frame -> frame.getMethod().getType().getName().equals("io.quotaflow.core.verification.KnownMonitorPin"))).toList();
        if (Runtime.version().feature() == 21) assertFalse(pins.isEmpty(), "negative control must detect the intentionally pinned virtual caller");
        else assertTrue(pins.isEmpty(), "monitor parking must not pin on the newer runtime");
    }


    @org.junit.jupiter.api.Test void attributionCannotHideUnknownOrLibraryBlocking() {
        assertEquals(PinningEvidence.Attribution.JVM_INITIALIZATION, PinningEvidence.classify("", List.of("java.util.concurrent.ConcurrentHashMap#putVal", "java.lang.ClassLoader#getClassLoadingLock")));
        assertEquals(PinningEvidence.Attribution.JVM_INITIALIZATION, PinningEvidence.classify("", List.of("java.lang.ClassLoader#defineClass0")));
        assertEquals(PinningEvidence.Attribution.UNATTRIBUTED, PinningEvidence.classify("", List.of()));
        assertEquals(PinningEvidence.Attribution.LIBRARY_BLOCKING, PinningEvidence.classify("", List.of("java.lang.Thread#sleep", "io.quotaflow.core.verification.KnownMonitorPin#park")));
        assertEquals(PinningEvidence.Attribution.LIBRARY_BLOCKING, PinningEvidence.classify("", List.of("io.quotaflow.core.Block#wait", "java.lang.ClassLoader#loadClass")));
        assertEquals(PinningEvidence.Attribution.JVM_INITIALIZATION, PinningEvidence.classify("", List.of("jdk.internal.loader.BuiltinClassLoader#loadClassOrNull", "io.quotaflow.core.Entry#acquire")));
        assertEquals(PinningEvidence.Attribution.JVM_INITIALIZATION, PinningEvidence.classify("Waited for initialization of io.quotaflow.core.Clock by another thread", List.of("io.quotaflow.core.Entry#acquire")));
        assertEquals(PinningEvidence.Attribution.LIBRARY_BLOCKING, PinningEvidence.classify("Waited for initialization of io.quotaflow.core.Clock by another thread", List.of("io.quotaflow.core.Clock#<clinit>")));
    }

    @org.junit.jupiter.api.Test void onlyJvmOwnedCallSiteLinkageCanUseTheColdInitializationCategory() {
        String cache = "java.lang.invoke.MethodTypeForm#setCachedLambdaForm";
        String linker = "java.lang.invoke.MethodHandleNatives#linkCallSiteImpl";
        assertEquals(PinningEvidence.Attribution.JVM_INITIALIZATION,
                PinningEvidence.classify("Native or VM frame on stack", List.of(cache, linker, "io.quotaflow.core.Entry#acquire")));
        assertEquals(PinningEvidence.Attribution.JVM_INITIALIZATION,
                PinningEvidence.classify("Native or VM frame on stack", List.of(cache,
                        "java.lang.invoke.DirectMethodHandle#preparedLambdaForm", "io.quotaflow.core.Entry#acquire")));
        assertEquals(PinningEvidence.Attribution.LIBRARY_BLOCKING,
                PinningEvidence.classify("Native or VM frame on stack", List.of("java.lang.Thread#sleep", "io.quotaflow.core.Block#wait", cache, linker)));
        assertEquals(PinningEvidence.Attribution.LIBRARY_BLOCKING,
                PinningEvidence.classify("Native or VM frame on stack", List.of(cache, "io.quotaflow.core.Block#wait", linker)));
        assertEquals(PinningEvidence.Attribution.LIBRARY_BLOCKING,
                PinningEvidence.classify("unknown", List.of(cache, linker, "io.quotaflow.core.Entry#acquire")));
    }

    @org.junit.jupiter.api.Test void aBlockingLibraryInitializerIsNotExcusedAsVmInitialization() throws Exception {
        Path path = Path.of("build/reports/virtual-threads", "jdk" + Runtime.version().feature(), "blocking-initializer-control.jfr");
        Files.createDirectories(path.getParent());
        try (var recording = new Recording()) {
            recording.enable("jdk.VirtualThreadPinned").withThreshold(Duration.ZERO).withStackTrace();
            recording.start();
            Thread.ofVirtual().start(io.quotaflow.core.verification.KnownBlockingInitialization::touch).join();
            recording.stop(); recording.dump(path);
        }
        assertTrue(PinningEvidence.read(path).library() > 0, "library-owned initialization blocking must remain a failure");
    }
}
