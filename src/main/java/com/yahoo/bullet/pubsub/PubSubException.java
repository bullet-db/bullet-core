package com.yahoo.bullet.pubsub;

/**
 * Exception to be thrown if there is an error in {@link PubSub}, {@link Publisher} or {@link Subscriber}.
 */
public class PubSubException extends Exception {
    /**
     * Constructor to initialize PubSubException with a message.
     *
     * @param message The error message to be associated with the PubSubException.
     */
    public PubSubException(String message) {
        super(message);
    }

    /**
     * Constructor to initialize PubSubException with a message and a {@link Throwable} cause.
     *
     * @param message The error message to be associated with the PubSubException.
     * @param cause The reason for the PubSubException.
     */
    public PubSubException(String message, Throwable cause) {
        super(message, cause);
    }
}
