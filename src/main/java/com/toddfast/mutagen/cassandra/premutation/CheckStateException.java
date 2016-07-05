package com.toddfast.mutagen.cassandra.premutation;

/**
 * Should be thrown in Premutation.check() method in case of
 * check failure
 */
public class CheckStateException extends RuntimeException {
    public CheckStateException(String message) {
        super(message);
    }

    public CheckStateException(String message, Throwable cause) {
        super(message, cause);
    }
}
