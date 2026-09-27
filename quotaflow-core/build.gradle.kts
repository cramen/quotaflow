plugins {
    `java-library`
    jacoco
    alias(libs.plugins.pitest)
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
    // The only dependency visible to consumers of the framework-free core.
    api(libs.slf4j.api)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)

    // JUnit 5 engine for PIT mutation testing.
    testImplementation(libs.pitest.junit5.plugin)
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// --- Coverage gate: branch coverage >= 90% on core.
jacoco {
    toolVersion = "0.8.12"
}

tasks.jacocoTestReport {
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}

tasks.jacocoTestCoverageVerification {
    violationRules {
        rule {
            element = "BUNDLE"
            limit {
                counter = "BRANCH"
                value = "COVEREDRATIO"
                minimum = "0.90".toBigDecimal()
            }
        }
    }
}

tasks.check { dependsOn(tasks.jacocoTestCoverageVerification) }

// --- Mutation testing for correctness paths. On demand only: not part of check.
pitest {
    targetClasses.set(setOf("io.quotaflow.core.*"))
    mutationThreshold.set(80)
}

// --- JMH: local fallback decision latency, gated absolutely at p99 <= 0.01 ms
// by benchmarkGate. The gate is a standalone verification task; CI runs it in
// a single designated job (not the JDK matrix).
jmh {
    fork = 1
    warmupIterations = 2
    iterations = 3
    resultFormat.set("JSON")
    resultsFile.set(layout.buildDirectory.file("reports/jmh/results.json"))
}

val fallbackP99GateMs = mapOf("io.quotaflow.core.LocalFallbackBenchmark" to 0.01)

tasks.register("benchmarkGate") {
    group = "verification"
    description = "Runs the JMH suite and fails if a gated p99 latency exceeds its absolute threshold."
    dependsOn("jmh")
    val results = layout.buildDirectory.file("reports/jmh/results.json")
    doLast {
        val entries = groovy.json.JsonSlurper().parse(results.get().asFile) as List<*>
        val failures = mutableListOf<String>()
        for ((benchmark, maxP99Ms) in fallbackP99GateMs) {
            val entry = entries.map { it as Map<*, *> }
                .singleOrNull { (it["benchmark"] as String).startsWith(benchmark) }
            if (entry == null) {
                failures += "$benchmark: no JMH result found"
                continue
            }
            val metric = entry["primaryMetric"] as Map<*, *>
            val p99 = ((metric["scorePercentiles"] as Map<*, *>)["99.0"] as Number).toDouble()
            if (p99 > maxP99Ms) {
                failures += "$benchmark: p99 latency $p99 ms exceeds the absolute gate of $maxP99Ms ms"
            } else {
                logger.lifecycle("benchmarkGate OK: $benchmark p99 = $p99 ms (gate <= $maxP99Ms ms)")
            }
        }
        if (failures.isNotEmpty()) {
            throw GradleException("Benchmark gate failed:\n" + failures.joinToString("\n"))
        }
    }
}
