package io.datadynamics.nifi.db.putdatabaserecord;

/**
 * Represents a looping process was discontinued.
 * When a method throws this exception, its caller should stop processing further inputs and stop immediately.
 */
public class DiscontinuedException extends RuntimeException {
    public DiscontinuedException(String message) {
        super(message);
    }

    public DiscontinuedException(String message, Throwable cause) {
        super(message, cause);
    }
}