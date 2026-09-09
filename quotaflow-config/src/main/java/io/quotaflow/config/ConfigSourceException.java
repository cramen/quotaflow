package io.quotaflow.config;

/** A configuration source could not be read. The serving policy set stays unchanged. */
public class ConfigSourceException extends RuntimeException {

    public ConfigSourceException(String message, Throwable cause) {
        super(message, cause);
    }
}
