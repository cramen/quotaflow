import com.vanniktech.maven.publish.MavenPublishBaseExtension
import org.gradle.api.artifacts.component.ModuleComponentIdentifier
import org.gradle.plugins.signing.Sign

plugins {
    // Root project only aggregates; module-specific config lives in each module.
    // Publishing/SBOM plugins are declared here (unapplied) so subprojects can
    // apply them by id from the shared configuration below. The Kotlin plugin
    // must share their classloader (required by the publish plugin when a
    // Kotlin module applies it), so it is declared here too.
    alias(libs.plugins.vanniktech.maven.publish) apply false
    alias(libs.plugins.cyclonedx.bom) apply false
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.dokka) apply false
}

// Short public description of each published module, used in the POM.
// quotaflow-tck (chaos/conformance harness) and quotaflow-native-smoke
// (internal GraalVM verification workload) are not published.
val moduleDescriptions = mapOf(
    "quotaflow-core" to "Framework-free policy engine core of the Quotaflow distributed rate limiter",
    "quotaflow-store-redis" to "Redis/Valkey-backed distributed counter storage (atomic Lua token bucket and GCRA) for Quotaflow",
    "quotaflow-fallback" to "Conservative local fallback limiter for the Quotaflow degradation mode",
    "quotaflow-config" to "Dynamic configuration, hot-reload and tariff resolver SPI for Quotaflow",
    "quotaflow-spring-boot-starter" to "Spring Boot starter for Quotaflow: @RateLimited annotation, auto-configuration, HTTP 429 semantics",
    "quotaflow-kotlin" to "Kotlin coroutines facade (suspend/Flow/DSL) for Quotaflow",
    "quotaflow-micrometer" to "Micrometer metrics/tracing bridges and the reference Grafana dashboard for Quotaflow",
)

subprojects {
    dependencyLocking {
        lockAllConfigurations()
        lockMode.set(LockMode.STRICT)
    }
    group = "io.quotaflow"
    // The repository stays on a snapshot version between releases; the release
    // workflow passes -Pversion=<tag without the v prefix>.
    version = providers.gradleProperty("version").orElse("0.1.0-SNAPSHOT").get()

    repositories {
        mavenCentral()
    }

    plugins.withId("java-library") {
        apply(from = rootProject.file("gradle/test-runtime.gradle"))
        tasks.withType<JavaCompile>().configureEach { options.encoding = "UTF-8" }
        tasks.withType<Javadoc>().configureEach {
            options.encoding = "UTF-8"
            (options as StandardJavadocDocletOptions).apply {
                charSet = "UTF-8"
                docEncoding = "UTF-8"
            }
        }
        tasks.withType<AbstractArchiveTask>().configureEach {
            isPreserveFileTimestamps = false
            isReproducibleFileOrder = true
        }
        // Dependency audit: each module declares an allowlist of group:module
        // entries (the "group:*" wildcard matches any artifact of that group) via
        // extra["dependencyAuditAllowlist"]; anything else on the runtime
        // classpath fails the build.
        val dependencyAudit = tasks.register("dependencyAudit") {
            description = "Fails if the runtime classpath exposes artifacts outside this module's allowlist."
            group = "verification"

            val runtimeClasspath = configurations.named("runtimeClasspath")

            doLast {
                val allowlist = (project.extra["dependencyAuditAllowlist"] as List<*>).map { it.toString() }
                val found = runtimeClasspath.get().incoming.artifactView { }.artifacts.artifacts.mapNotNull { artifact ->
                    val id = artifact.variant.owner as? ModuleComponentIdentifier
                    id?.let { "${it.group}:${it.module}" }
                }.toSortedSet()

                val unexpected = found.filter { artifact ->
                    allowlist.none { entry ->
                        entry == artifact || (entry.endsWith(":*") && artifact.startsWith(entry.dropLast(1)))
                    }
                }
                if (unexpected.isNotEmpty()) {
                    throw GradleException(
                        "${project.path} must not expose artifacts outside its allowlist. " +
                            "Unexpected modules on runtime classpath: $unexpected"
                    )
                }
                logger.lifecycle("dependencyAudit OK: ${project.path} exposes $found")
            }
        }

        tasks.named("check") { dependsOn(dependencyAudit) }

        // Publishing + SBOM wiring for the published modules only; the TCK is
        // an internal test harness and never leaves the repository.
        val moduleDescription = moduleDescriptions[project.name]
        if (moduleDescription != null) {
            apply(plugin = "com.vanniktech.maven.publish")
            apply(plugin = "org.cyclonedx.bom")

            extensions.configure<MavenPublishBaseExtension> {
                // Central Portal is the only supported upload path since the
                // legacy OSSRH sunset (the plugin's default host). The release
                // promotion adapter uploads a previously verified immutable
                // bundle; direct Gradle Central upload tasks are disabled.
                publishToMavenCentral()
                // Signing is required by Central for non-snapshot versions; the
                // Sign tasks below additionally no-op unless key material is
                // present, so local builds never need keys.
                if (!providers.gradleProperty("releasePreparation").orElse("false").get().toBoolean()) {
                    signAllPublications()
                }

                pom {
                    name.set("Quotaflow ${project.name.removePrefix("quotaflow-")}")
                    description.set(moduleDescription)
                    url.set("https://github.com/cramen/quotaflow")
                    licenses {
                        license {
                            name.set("Apache-2.0")
                            url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                        }
                    }
                    developers {
                        developer {
                            id.set("cramen")
                            name.set("cramen")
                            url.set("https://github.com/cramen")
                        }
                    }
                    scm {
                        url.set("https://github.com/cramen/quotaflow")
                        connection.set("scm:git:git://github.com/cramen/quotaflow.git")
                        developerConnection.set("scm:git:ssh://git@github.com/cramen/quotaflow.git")
                    }
                }
            }

            if (providers.gradleProperty("verificationRepository").isPresent) {
                extensions.configure<org.gradle.api.publish.PublishingExtension> {
                    repositories.maven {
                        name = "Verification"
                        url = rootProject.uri(providers.gradleProperty("verificationRepository").get())
                    }
                }
            }

            // Signing happens only in the release context: the armored PGP
            // private key reaches the build exclusively via the
            // ORG_GRADLE_PROJECT_signingInMemoryKey* environment variables
            // populated from CI secrets. Without them the Sign tasks skip, so
            // contributors and snapshot builds never need key material, and a
            // release without keys publishes nothing (Central rejects unsigned
            // deployments, and the release workflow does not even attempt the
            // publish when the secrets are absent).
            tasks.withType<Sign>().configureEach {
                onlyIf("PGP signing key material is present (release context)") {
                    providers.environmentVariable("ORG_GRADLE_PROJECT_signingInMemoryKey").isPresent ||
                        providers.gradleProperty("signingInMemoryKey").isPresent
                }
            }
        }
    }
}

apply(from = "gradle/release-preparation.gradle")


tasks.register("correctnessGate") {
    group = "verification"
    description = "Requires independent core and fallback mutation thresholds."
    dependsOn(":quotaflow-core:pitest", ":quotaflow-fallback:pitest")
    doLast {
        val factory = javax.xml.parsers.DocumentBuilderFactory.newInstance().apply {
            setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
            isExpandEntityReferences = false
        }
        for (module in listOf("core", "fallback")) {
            val report = file("quotaflow-$module/build/reports/pitest/mutations.xml")
            val mutations = factory.newDocumentBuilder().parse(report).getElementsByTagName("mutation")
            var detected = 0
            for (index in 0 until mutations.length) {
                val attributes = mutations.item(index).attributes
                val status = attributes.getNamedItem("status").nodeValue
                if (status !in setOf("KILLED", "SURVIVED", "NO_COVERAGE", "TIMED_OUT", "NON_VIABLE"))
                    throw GradleException("$module mutation analysis is incomplete: $status")
                if (attributes.getNamedItem("detected").nodeValue == "true") detected++
            }
            // PIT rounds its displayed percentage. Release acceptance uses the exact ratio.
            if (mutations.length == 0 || detected.toLong() * 100 < mutations.length.toLong() * 80)
                throw GradleException("$module mutation score is below 80%: $detected/${mutations.length}")
            logger.lifecycle("$module mutation gate: $detected/${mutations.length} detected")
        }
    }
}

// CI may validate benchmark evidence, but the user-selected measurement host is local.
gradle.taskGraph.whenReady {
    if (providers.environmentVariable("GITHUB_ACTIONS").orElse("false").get() == "true" &&
        allTasks.any { it.name == "jmh" }) {
        throw GradleException("Benchmark execution is local-only; GitHub may validate imported evidence.")
    }
}
