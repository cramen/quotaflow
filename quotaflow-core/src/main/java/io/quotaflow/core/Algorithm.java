package io.quotaflow.core;

/** Rate limiting algorithm applied per policy level. */
public enum Algorithm {
    TOKEN_BUCKET,
    GCRA
}
