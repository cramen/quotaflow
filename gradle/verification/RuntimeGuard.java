package io.quotaflow.verification;

import java.lang.instrument.Instrumentation;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

/** Build-only agent: verifies the actual worker before test frameworks initialize. */
public final class RuntimeGuard {
    private RuntimeGuard() { }

    public static void premain(String arguments, Instrumentation ignored) throws Exception {
        String[] parts = arguments.split(",", 2);
        if (parts.length != 2) throw new IllegalArgumentException("Expected JDK and report directory");
        int expected = Integer.parseInt(parts[0]);
        int actual = Runtime.version().feature();
        Path reports = Path.of(parts[1]);
        Files.createDirectories(reports);
        String status = actual == expected ? "PASS" : "FAIL";
        String report = "{\"expectedJdk\":" + expected + ",\"actualJdk\":" + actual
                + ",\"status\":" + json(status) + ",\"runtimeVersion\":" + json(Runtime.version().toString())
                + ",\"javaHome\":" + json(System.getProperty("java.home"))
                + ",\"vendor\":" + json(System.getProperty("java.vendor"))
                + ",\"pid\":" + ProcessHandle.current().pid() + ",\"observedAt\":" + json(Instant.now().toString()) + "}\n";
        Files.writeString(reports.resolve("worker-" + ProcessHandle.current().pid() + ".json"), report);
        System.out.println("Quotaflow test worker: expected JDK " + expected + ", actual " + Runtime.version() + " (" + status + ")");
        if (actual != expected) {
            System.err.println("Test worker JDK mismatch: expected " + expected + ", actual " + actual);
            System.exit(78);
        }
    }

    public static void main(String[] ignored) { }

    private static String json(String value) {
        StringBuilder result = new StringBuilder("\"");
        for (char character : value.toCharArray()) {
            switch (character) {
                case '\\' -> result.append("\\\\");
                case '"' -> result.append("\\\"");
                default -> {
                    if (character < 32) result.append(String.format("\\u%04x", (int) character));
                    else result.append(character);
                }
            }
        }
        return result.append('"').toString();
    }
}
