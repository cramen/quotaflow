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
    "redis.clients.authentication:*"
)

dependencies {
    api(project(":quotaflow-core"))
    api(project(":quotaflow-store-redis"))

    implementation(platform(libs.spring.boot.dependencies))
    implementation(libs.spring.boot.starter)

    testImplementation(platform(libs.spring.boot.dependencies))
    testImplementation(libs.spring.boot.starter.test)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.withType<Test> {
    useJUnitPlatform()
}
