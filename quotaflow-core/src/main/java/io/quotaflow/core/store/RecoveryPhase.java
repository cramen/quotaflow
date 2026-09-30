package io.quotaflow.core.store;

/** Authoritative server phase; DRAIN never admits a business acquisition. */
public enum RecoveryPhase { NORMAL, GATHER, DRAIN }
