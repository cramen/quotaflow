package io.quotaflow.core;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** Optional asynchronous tariff lookup; invocation must return promptly. Caller cancellation must not cancel shared work. */
public interface AsyncLimitResolver extends LimitResolver {
    CompletionStage<Optional<Limit>> resolveAsync(String limitRef, String keyGroup);
}
