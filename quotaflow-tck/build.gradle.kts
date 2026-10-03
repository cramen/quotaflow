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
    testImplementation(testFixtures(project(":quotaflow-store-redis")))
    implementation(project(":quotaflow-core"))
    implementation(project(":quotaflow-store-redis"))
    implementation(project(":quotaflow-fallback"))
    implementation(project(":quotaflow-config"))
    implementation(project(":quotaflow-spring-boot-starter"))
    implementation(project(":quotaflow-kotlin"))
    implementation(project(":quotaflow-micrometer"))

    testImplementation(libs.jackson.databind)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
}

tasks.withType<Test> {
    useJUnitPlatform()
    systemProperty("quotaflow.tck.classpath", sourceSets["test"].runtimeClasspath.asPath)
}

// --- Soak profile (D5): NOT part of check — on-demand and nightly only.
// Sustained mixed traffic with periodic Redis pause/unpause degradation
// injection and continuous invariant assertions. Default one hour; the
// nightly workflow runs 1 h and reports environment-specific probe latency.
tasks.register<JavaExec>("soakTest") {
    group = "verification"
    description = "Runs the soak profile: sustained mixed traffic with periodic degradation injection " +
        "(default 1 h; -PsoakDurationSeconds=60 for a diagnostic run)."
    mainClass.set("io.quotaflow.tck.SoakHarness")
    classpath = sourceSets["test"].runtimeClasspath
    // fixed heap ceiling: unbounded state growth would surface as OOM
    jvmArgs("-Xmx512m")
    systemProperty("quotaflow.soak.duration.seconds",
        providers.gradleProperty("soakDurationSeconds").orElse("3600").get())
}

// Virtual-thread sources are compiled only with a capable JDK, never reflection-skipped.
val verificationJdk = providers.gradleProperty("testJdk").orElse("17").get().toInt()
if (verificationJdk >= 21) {
    val virtualTest = sourceSets.create("virtualTest") {
        compileClasspath += sourceSets.test.get().output
        runtimeClasspath += sourceSets.test.get().output
    }
    configurations[virtualTest.implementationConfigurationName].extendsFrom(configurations.testImplementation.get())
    configurations[virtualTest.runtimeOnlyConfigurationName].extendsFrom(configurations.testRuntimeOnly.get())
    dependencies {
        add(virtualTest.implementationConfigurationName, testFixtures(project(":quotaflow-fallback")))
    }
    tasks.named<JavaCompile>(virtualTest.compileJavaTaskName) {
        javaCompiler = javaToolchains.compilerFor { languageVersion = JavaLanguageVersion.of(verificationJdk) }
        options.release.set(21)
    }
    val virtualThreads = tasks.register<Test>("virtualTest") {
        description = "Runs 100,000 virtual callers per algorithm/path and verifies JFR pinning attribution."
        testClassesDirs = virtualTest.output.classesDirs
        include("**/*VirtualTest.class", "**/PinningControlTest.class")
        classpath = virtualTest.runtimeClasspath
        useJUnitPlatform()
        maxHeapSize = "1g"
        val phaseReports = layout.buildDirectory.dir("reports/virtual-threads/jdk$verificationJdk")
        outputs.dir(phaseReports)
        doFirst { delete(phaseReports) }
        forkEvery = 1
        maxParallelForks = 1
        shouldRunAfter(tasks.test)
        systemProperty("quotaflow.tck.classpath", virtualTest.runtimeClasspath.asPath)
    }
    tasks.check { dependsOn(virtualThreads) }
}

// Real failover/redirection is required on both supported server implementations.
val valkeyTopology = tasks.register<Test>("valkeyTopologyTest") {
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    include("**/ClusterTopologyTckTest.class", "**/SentinelTopologyTckTest.class")
    systemProperty("quotaflow.topology.valkey", "true")
    shouldRunAfter(tasks.test)
}
tasks.check { dependsOn(valkeyTopology) }
