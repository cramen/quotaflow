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
}

tasks.withType<Test> {
    useJUnitPlatform()
}
