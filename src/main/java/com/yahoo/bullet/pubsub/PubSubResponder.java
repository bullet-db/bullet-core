/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

/**
 * This is used by any class that needs to respond to a {@link PubSubMessage}.
 */
public interface PubSubResponder extends AutoCloseable {
    /**
     * Respond to a {@link PubSubMessage}.
     *
     * @param id The id of the response.
     * @param message The actual {@link PubSubMessage} containing the response.
     */
    void respond(String id, PubSubMessage message);

    @Override
    default void close() {
    }
}
