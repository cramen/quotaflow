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
extra["dependencyAuditAllowlist"] = listOf(
    "org.slf4j:slf4j-api",
    "io.lettuce:lettuce-core",
    "io.netty:*",
    "io.projectreactor:reactor-core",
    "org.reactivestreams:reactive-streams",
    "redis.clients.authentication:*"
)

dependencies {
    api(project(":quotaflow-core"))
    api(libs.lettuce.core)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    // Reproduces the incomplete epoll stack (4.1 transport classes against the
    // 4.2 core Lettuce resolves) for the native-transport guard tests. Classes
    // only — the jar carries no native library, so Epoll stays unavailable and
    // every client in this suite still runs on NIO.
    testImplementation(libs.netty.transport.native.epoll.legacy)

    jmhImplementation(libs.testcontainers)
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// --- JMH: distributed allow-path chain evaluation. Gates (benchmarkGate):
// throughput >= 50k decisions/s absolutely, plus throughput and p99 latency
// within 10% of the versioned baseline (benchmark-baseline.json). Refresh the
// baseline intentionally with -PupdateBenchmarkBaseline; the diff is review-
// visible. The committed baseline is seeded from a local reference run;
// refresh it from the CI benchmark runner if its profile differs materially.
// CI runs the gate in a single designated job (not the JDK matrix).
jmh {
    fork = 1
    warmupIterations = 2
    iterations = 3
    resultFormat.set("JSON")
    resultsFile.set(layout.buildDirectory.file("reports/jmh/results.json"))
}

val minChainThroughputOpsPerSec = 50_000.0
val benchmarkRegressionBudget = 0.10
val benchmarkBaselineFile = layout.projectDirectory.file("benchmark-baseline.json")
val updateBenchmarkBaseline = providers.gradleProperty("updateBenchmarkBaseline").isPresent

tasks.register("benchmarkGate") {
    group = "verification"
    description = "Runs the JMH suite and fails on throughput below 50k decisions/s or a regression beyond 10% versus the versioned baseline."
    dependsOn("jmh")
    val results = layout.buildDirectory.file("reports/jmh/results.json")
    doLast {
        val entries = (groovy.json.JsonSlurper().parse(results.get().asFile) as List<*>).map { it as Map<*, *> }
        fun primaryMetric(benchmark: String) = entries
            .singleOrNull { (it["benchmark"] as String).startsWith(benchmark) }
            ?.get("primaryMetric") as Map<*, *>?
        val throughput = primaryMetric("io.quotaflow.store.redis.RedisChainBenchmark.chainAllowThroughput")
            ?.get("score") as Number?
        val latencyP99 = (primaryMetric("io.quotaflow.store.redis.RedisChainBenchmark.chainAllowLatency")
            ?.get("scorePercentiles") as Map<*, *>?)
            ?.get("99.0") as Number?

        val failures = mutableListOf<String>()
        if (throughput == null || latencyP99 == null) {
            failures += "missing JMH results (throughput=$throughput, latencyP99=$latencyP99)"
        } else {
            // metric name -> regresses when the measured value is HIGHER (latency),
            // lower (throughput) than the baseline beyond the budget
            val measured = mapOf(
                "redisChainThroughputOpsPerSec" to (throughput.toDouble() to false),
                "redisChainLatencyP99Ms" to (latencyP99.toDouble() to true)
            )
            if (throughput.toDouble() < minChainThroughputOpsPerSec) {
                failures += "redisChainThroughputOpsPerSec: ${throughput.toDouble()} decisions/s is below" +
                    " the absolute gate of $minChainThroughputOpsPerSec decisions/s"
            }
            if (updateBenchmarkBaseline) {
                benchmarkBaselineFile.asFile.writeText(
                    groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(
                        measured.mapValues { it.value.first })) + "\n")
                logger.lifecycle("benchmarkGate: baseline updated from this run -> $benchmarkBaselineFile" +
                    " ($measured); review the diff before committing")
            } else if (!benchmarkBaselineFile.asFile.isFile) {
                failures += "no benchmark baseline at $benchmarkBaselineFile; seed it with" +
                    " './gradlew :quotaflow-store-redis:benchmarkGate -PupdateBenchmarkBaseline'"
            } else {
                val baseline = groovy.json.JsonSlurper().parse(benchmarkBaselineFile.asFile) as Map<*, *>
                for ((metric, measuredAndDirection) in measured) {
                    val (value, higherIsRegression) = measuredAndDirection
                    val reference = (baseline[metric] as Number?)?.toDouble()
                    val regressed = reference != null && if (higherIsRegression) {
                        value > reference * (1.0 + benchmarkRegressionBudget)
                    } else {
                        value < reference * (1.0 - benchmarkRegressionBudget)
                    }
                    if (reference == null) {
                        failures += "$metric: not present in the baseline file"
                    } else if (regressed) {
                        failures += "$metric: regressed beyond 10% (baseline $reference, measured $value)"
                    } else {
                        logger.lifecycle("benchmarkGate OK: $metric = $value (baseline $reference)")
                    }
                }
            }
        }
        if (failures.isNotEmpty()) {
            throw GradleException("Benchmark gate failed:\n" + failures.joinToString("\n"))
        }
    }
}
