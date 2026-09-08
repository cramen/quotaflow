import org.gradle.api.artifacts.component.ModuleComponentIdentifier

plugins {
    // Root project only aggregates; module-specific config lives in each module.
}

subprojects {
    group = "io.quotaflow"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }

    plugins.withId("java-library") {
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
                val found = runtimeClasspath.get().incoming.artifactView {
                    lenient(true)
                }.artifacts.artifacts.mapNotNull { artifact ->
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
    }
}
