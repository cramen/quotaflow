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

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
}

tasks.withType<Test> {
    useJUnitPlatform()
}
