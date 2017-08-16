package com.yahoo.bullet.pubsub;

public class PubSubException extends Exception {
    /**
     * Exception to be thrown if there is an error in {@link PubSub}, {@link Publisher} or {@link Subscriber}.
     *
     * @param message The error message to be associated with the PubSubException.
     */
    public PubSubException(String message) {
        super(message);
    }
}
