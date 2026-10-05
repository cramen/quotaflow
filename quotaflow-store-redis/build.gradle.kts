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
    testFixturesApi(project(":quotaflow-core"))
    testFixturesApi(libs.lettuce.core)
    api(libs.lettuce.core)
    api(platform(libs.netty.bom))

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    // Reproduces the incomplete epoll stack (4.1 transport classes against the
    // 4.2 core Lettuce resolves) for the native-transport guard tests. Classes
    // only — the jar carries no native library, so Epoll stays unavailable and
    // every client in this suite still runs on NIO.
    testImplementation(libs.netty.transport.native.epoll.legacy) {
        version { strictly(libs.versions.netty.legacy.get()) }
    }
    testImplementation("io.netty:netty-transport-classes-epoll") {
        version { strictly(libs.versions.netty.legacy.get()) }
    }

    jmhImplementation(libs.testcontainers)
}

tasks.withType<Test> {
    systemProperty("quotaflow.test.redis.image", providers.gradleProperty("testRedisImage").orElse("redis:6.2.24-alpine").get())
    useJUnitPlatform()
}

// JMH measurements are environment-specific; comparison requires an explicit matching baseline.
jmh {
    benchmarkParameters.put("algorithm", objects.listProperty(String::class.java).value(listOf(providers.gradleProperty("benchmarkAlgorithm").orElse("TOKEN_BUCKET").get())))
    fork = 1
    warmupIterations = 2
    iterations = 3
    resultFormat.set("JSON")
    resultsFile.set(layout.buildDirectory.file("reports/jmh/results.json"))
}

extra["benchmarkMetrics"] = mapOf(
    "redisChainThroughputOpsPerSec" to mapOf("benchmark" to "io.quotaflow.store.redis.RedisChainBenchmark.chainAllowThroughput",
        "mode" to "thrpt", "unit" to "ops/s"),
    "redisChainLatencyP99Ms" to mapOf("benchmark" to "io.quotaflow.store.redis.RedisChainBenchmark.chainAllowLatency",
        "mode" to "sample", "unit" to "ms/op", "percentile" to "99.0")
)
apply(from = rootProject.file("gradle/benchmark-verification.gradle"))
