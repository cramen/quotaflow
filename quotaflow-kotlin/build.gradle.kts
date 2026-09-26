plugins {
    `java-library`
    alias(libs.plugins.kotlin.jvm)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

kotlin {
    jvmToolchain(17)
}

// Runtime classpath allowlist enforced by the dependencyAudit task (see root build).
extra["dependencyAuditAllowlist"] = listOf(
    "org.slf4j:slf4j-api",
    "org.jetbrains.kotlinx:*",
    "org.jetbrains.kotlin:*",
    "org.jetbrains:annotations"
)

dependencies {
    api(project(":quotaflow-core"))
    api(libs.kotlinx.coroutines.core)
    api(libs.kotlinx.coroutines.jdk8)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(project(":quotaflow-config"))
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.withType<Test> {
    useJUnitPlatform()
}
