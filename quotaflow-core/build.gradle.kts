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

// --- JMH: placeholder benchmark only; blocking regression thresholds arrive
// with a real hot path.
jmh {
    fork = 1
    warmupIterations = 1
    iterations = 1
}
