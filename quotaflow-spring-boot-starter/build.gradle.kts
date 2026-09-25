plugins {
    `java-library`
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

// Runtime classpath allowlist enforced by the dependencyAudit task (see root build).
// Deliberately no org.aspectj:aspectjweaver: the annotation machinery works on
// plain Spring AOP proxies, so consuming modules keep their slim footprint.
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
    "redis.clients.authentication:*"
)

dependencies {
    api(project(":quotaflow-core"))
    api(project(":quotaflow-store-redis"))
    api(project(":quotaflow-fallback"))
    api(project(":quotaflow-config"))
    api(project(":quotaflow-micrometer"))

    // Exposed so consumers resolve aligned Spring versions for the api deps below.
    api(platform(libs.spring.boot.dependencies))

    // Plain Spring AOP (no AspectJ weaver): the auto-configuration registers
    // the infrastructure auto-proxy creator itself, so @RateLimited works in
    // any app and escalates cleanly when the app enables full AspectJ proxying.
    api("org.springframework:spring-aop")

    implementation(platform(libs.spring.boot.dependencies))
    implementation(libs.spring.boot.starter)

    // Web stacks, reactor and Spring Security are detected at runtime: the
    // servlet handler, reactive handler, reactive interceptor path and
    // principal seeding activate only when the app brings the matching stack.
    compileOnly("org.springframework.boot:spring-boot-starter-web")
    compileOnly("org.springframework.boot:spring-boot-starter-webflux")
    compileOnly("io.projectreactor:reactor-core")
    compileOnly("org.springframework.security:spring-security-core")

    testImplementation(platform(libs.spring.boot.dependencies))
    testImplementation(libs.spring.boot.starter.test)
    testImplementation("org.springframework.boot:spring-boot-starter-web")
    testImplementation("org.springframework.boot:spring-boot-starter-webflux")
    testImplementation("org.springframework.security:spring-security-core")
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Parameter names power SpEL keys such as `key = "#userId"`; Spring Boot apps
// compile with -parameters by default and this module does the same.
tasks.withType<JavaCompile> {
    options.compilerArgs.add("-parameters")
}

tasks.withType<Test> {
    useJUnitPlatform()
}
