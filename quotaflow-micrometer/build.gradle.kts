plugins {
    `java-library`
    alias(libs.plugins.jmh)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

// Runtime classpath allowlist enforced by the dependencyAudit task (see root build).
// Micrometer artifacts are confined to this module; the fallback and tracing
// bridges are compileOnly so they never leak onto consumer classpaths.
// HdrHistogram and LatencyUtils are micrometer-core's own runtime companions.
extra["dependencyAuditAllowlist"] = listOf(
    "org.slf4j:slf4j-api",
    "io.micrometer:*",
    "org.hdrhistogram:HdrHistogram",
    "org.latencyutils:LatencyUtils"
)

dependencies {
    api(project(":quotaflow-core"))
    api(libs.micrometer.core)

    // Optional bridges: the degradation listener needs the fallback listener
    // type, the tracing listener needs micrometer-tracing. Both are opt-in —
    // neither is exposed to consumers transitively.
    compileOnly(project(":quotaflow-fallback"))
    compileOnly(libs.micrometer.tracing)

    testImplementation(project(":quotaflow-fallback"))
    testImplementation(testFixtures(project(":quotaflow-fallback")))
    testImplementation(libs.micrometer.tracing)
    testImplementation(libs.micrometer.tracing.test)
    testImplementation(libs.jackson.databind)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// Diagnostic runs include real observation delivery; comparison needs a reviewed matching baseline.
jmh {
    benchmarkParameters.put("algorithm", objects.listProperty(String::class.java).value(listOf(providers.gradleProperty("benchmarkAlgorithm").orElse("TOKEN_BUCKET").get())))
    fork = 1
    warmupIterations = 2
    iterations = 3
    resultFormat.set("JSON")
    resultsFile.set(layout.buildDirectory.file("reports/jmh/results.json"))
}
