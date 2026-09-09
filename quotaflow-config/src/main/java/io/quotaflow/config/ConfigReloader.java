package io.quotaflow.config;

import io.quotaflow.core.DefaultQuotaFlow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hot-reload pipeline for rate limit configuration: load from a
 * {@link ConfigSource}, parse and compile (fail-fast validation through
 * {@code PolicySet.compile}), then atomically swap the serving policy set on
 * the {@link DefaultQuotaFlow} facade. A payload that fails to load or
 * compile never takes effect: the old set keeps serving and the error is
 * logged with the offending entries.
 *
 * <p>Two triggers feed the same pipeline: a polling watcher (default 500 ms,
 * configurable; portable and predictable in containers) started by
 * {@link #start()}, and the programmatic {@link #reload()} entry point. The
 * watcher compares payload content, so change detection does not depend on
 * file-system timestamp granularity. Hooks registered via
 * {@link Builder#onApplied(Runnable)} run after every successful swap — for
 * example clearing a {@link CachingLimitResolver}, since a reload invalidates
 * cached resolutions.
 */
public final class ConfigReloader implements AutoCloseable {

    /** Default watcher poll interval; meets the one-second application target. */
    public static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMillis(500);

    private static final Logger log = LoggerFactory.getLogger(ConfigReloader.class);

    private final ConfigSource source;
    private final DefaultQuotaFlow quotaFlow;
    private final Duration pollInterval;
    private final List<Runnable> onApplied;
    private final Object pipelineLock = new Object();

    private volatile Map<String, String> lastSeen;
    private ScheduledExecutorService scheduler;

    private ConfigReloader(Builder builder) {
        this.source = builder.source;
        this.quotaFlow = builder.quotaFlow;
        this.pollInterval = builder.pollInterval;
        this.onApplied = List.copyOf(builder.onApplied);
    }

    public static Builder builder(ConfigSource source, DefaultQuotaFlow quotaFlow) {
        return new Builder(source, quotaFlow);
    }

    /**
     * Applies the current source payload immediately, then starts the polling
     * watcher (unless a zero/negative poll interval was configured). An
     * initial payload that fails to load or compile leaves the facade's
     * startup policy set serving.
     */
    public void start() {
        ReloadResult initial = reload();
        if (!initial.applied()) {
            log.warn("initial configuration load rejected; startup policy set keeps serving: {}",
                    initial.error());
        }
        if (!pollInterval.isZero() && !pollInterval.isNegative()) {
            scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "quotaflow-config-watcher");
                thread.setDaemon(true);
                return thread;
            });
            scheduler.scheduleWithFixedDelay(
                    this::pollSafely, pollInterval.toMillis(), pollInterval.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Runs the full pipeline once on demand: load, parse, compile, swap.
     * Never throws for source or validation problems; failures are reported
     * in the result and logged.
     */
    public ReloadResult reload() {
        Map<String, String> payload;
        try {
            payload = Map.copyOf(Objects.requireNonNull(source.load(), "source payload"));
        } catch (RuntimeException e) {
            log.error("configuration reload failed at source read; serving policy set unchanged: {}",
                    e.getMessage());
            return ReloadResult.rejected(e.getMessage());
        }
        return apply(payload);
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    private void pollSafely() {
        try {
            Map<String, String> payload = Map.copyOf(source.load());
            if (!payload.equals(lastSeen)) {
                apply(payload);
            }
        } catch (RuntimeException e) {
            log.warn("configuration source read failed; serving policy set unchanged: {}",
                    e.getMessage());
        }
    }

    private ReloadResult apply(Map<String, String> payload) {
        synchronized (pipelineLock) {
            lastSeen = payload;
            QuotaFlowConfiguration configuration;
            try {
                configuration = ConfigurationParser.parse(payload);
            } catch (RuntimeException e) {
                log.error("configuration reload rejected; serving policy set unchanged: {}",
                        e.getMessage());
                return ReloadResult.rejected(e.getMessage());
            }
            quotaFlow.replacePolicySet(configuration.policySet());
            for (Runnable hook : onApplied) {
                hook.run();
            }
            log.info("configuration applied: {} policies swapped atomically",
                    configuration.policySet().size());
            return ReloadResult.applied(configuration.policySet().size());
        }
    }

    public static final class Builder {
        private final ConfigSource source;
        private final DefaultQuotaFlow quotaFlow;
        private Duration pollInterval = DEFAULT_POLL_INTERVAL;
        private final List<Runnable> onApplied = new ArrayList<>();

        private Builder(ConfigSource source, DefaultQuotaFlow quotaFlow) {
            this.source = Objects.requireNonNull(source, "source");
            this.quotaFlow = Objects.requireNonNull(quotaFlow, "quotaFlow");
        }

        /**
         * Watcher poll interval; the default is {@link #DEFAULT_POLL_INTERVAL}.
         * A zero or negative interval disables watching ({@code reload()} only).
         */
        public Builder pollInterval(Duration pollInterval) {
            this.pollInterval = Objects.requireNonNull(pollInterval, "pollInterval");
            return this;
        }

        /** Hook executed after each successfully applied reload (e.g. resolver cache clear). */
        public Builder onApplied(Runnable hook) {
            onApplied.add(Objects.requireNonNull(hook, "hook"));
            return this;
        }

        public ConfigReloader build() {
            return new ConfigReloader(this);
        }
    }
}
