plugins {
    `java-library`
    `java-test-fixtures`
    alias(libs.plugins.jmh)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

// Runtime classpath allowlist enforced by the dependencyAudit task (see root build).
extra["dependencyAuditAllowlist"] = listOf("org.slf4j:slf4j-api")

dependencies {
    api(project(":quotaflow-core"))
    testFixturesApi(project(":quotaflow-core"))
    jmhImplementation(testFixtures(project(":quotaflow-fallback")))

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.withType<Test> {
    useJUnitPlatform()
}

jmh {
    benchmarkParameters.put("algorithm", objects.listProperty(String::class.java).value(listOf(providers.gradleProperty("benchmarkAlgorithm").orElse("TOKEN_BUCKET").get())))
    fork = 1
    warmupIterations = 2
    iterations = 3
    resultFormat.set("JSON")
    resultsFile.set(layout.buildDirectory.file("reports/jmh/results.json"))
}
extra["benchmarkMetrics"] = mapOf(
    "fallbackLatencyP99Ms" to mapOf("benchmark" to "io.quotaflow.fallback.FallbackDecisionBenchmark.acquire",
        "mode" to "sample", "unit" to "ms/op", "percentile" to "99.0")
)
apply(from = rootProject.file("gradle/benchmark-verification.gradle"))
