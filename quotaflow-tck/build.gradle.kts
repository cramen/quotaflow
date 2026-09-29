plugins {
    `java-library`
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

// Runtime classpath allowlist enforced by the dependencyAudit task (see root build).
extra["dependencyAuditAllowlist"] = listOf(
    "org.slf4j:*",
    "org.springframework:*",
    "org.springframework.boot:*",
    "ch.qos.logback:*",
    "org.apache.logging.log4j:*",
    "jakarta.annotation:*",
    "org.yaml:snakeyaml",
    "io.micrometer:*",
    "org.hdrhistogram:HdrHistogram",
    "org.latencyutils:LatencyUtils",
    "io.lettuce:lettuce-core",
    "io.netty:*",
    "io.projectreactor:reactor-core",
    "org.reactivestreams:reactive-streams",
    "redis.clients.authentication:*",
    "org.jetbrains.kotlinx:*",
    "org.jetbrains.kotlin:*",
    "org.jetbrains:annotations"
)

dependencies {
    implementation(project(":quotaflow-core"))
    implementation(project(":quotaflow-store-redis"))
    implementation(project(":quotaflow-fallback"))
    implementation(project(":quotaflow-config"))
    implementation(project(":quotaflow-spring-boot-starter"))
    implementation(project(":quotaflow-kotlin"))
    implementation(project(":quotaflow-micrometer"))

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
}

tasks.withType<Test> {
    useJUnitPlatform()
}

// --- Soak profile (D5): NOT part of check — on-demand and nightly only.
// Sustained mixed traffic with periodic Redis pause/unpause degradation
// injection and continuous invariant assertions. Default 5 minutes; the
// nightly workflow runs 1 h and reports environment-specific probe latency.
tasks.register<JavaExec>("soakTest") {
    group = "verification"
    description = "Runs the soak profile: sustained mixed traffic with periodic degradation injection " +
        "(default 5 min; -PsoakDurationSeconds=3600 for the nightly profile)."
    mainClass.set("io.quotaflow.tck.SoakHarness")
    classpath = sourceSets["test"].runtimeClasspath
    // fixed heap ceiling: unbounded state growth would surface as OOM
    jvmArgs("-Xmx512m")
    systemProperty("quotaflow.soak.duration.seconds",
        providers.gradleProperty("soakDurationSeconds").orElse("300").get())
}
