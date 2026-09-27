plugins {
    `java-library`
    application
    alias(libs.plugins.graalvm.native)
}

// No toolchain pin: the native-image build must run on the GraalVM JDK that
// runs Gradle (JAVA_HOME), both in the CI native-smoke job (setup-graalvm)
// and for local verification runs.

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
    implementation(project(":quotaflow-core"))
    implementation(project(":quotaflow-store-redis"))
}

application {
    mainClass.set("io.quotaflow.nativesmoke.NativeSmoke")
}

graalvmNative {
    // Lettuce, netty and this project's modules all carry their reachability
    // metadata in-jar; the external metadata repository is not needed (and
    // pinning its schema to the GraalVM release adds fragility).
    metadataRepository {
        enabled.set(false)
    }
    binaries {
        named("main") {
            mainClass.set("io.quotaflow.nativesmoke.NativeSmoke")
            sharedLibrary.set(false)
            // netty 4.2's CleanerJava25 releases direct buffers through a shared
            // FFM arena; without this GraalVM flag the event loop dies with
            // UnsupportedFeatureError on the first deallocation
            buildArgs.addAll(listOf("--no-fallback", "-H:+SharedArenaSupport"))
        }
    }
}
