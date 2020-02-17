/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
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

    /**
     * Method to create a PubSubException when a required argument could not be read.
     *
     * @param name The name of the argument that could not be read.
     * @param cause The optional {@link Throwable} that caused the exception.
     * @return A PubSubException indicating failure to read a required argument.
     */
    public static PubSubException forArgument(String name, Throwable cause) {
        String message = "Could not read required argument: " + name;
        return cause == null ? new PubSubException(message) : new PubSubException(message, cause);
    }
}
