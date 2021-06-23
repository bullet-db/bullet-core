/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import java.io.Serializable;

public interface Publisher extends AutoCloseable {
    /**
     * Send a message with an ID and content.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @return The sent {@link PubSubMessage}.
     * @throws PubSubException if the messaging system throws an error.
     */
    default PubSubMessage send(String id, Serializable content) throws PubSubException {
        return send(new PubSubMessage(id, content));
    }

    /**
     * Sends a {@link PubSubMessage}. The message might be modified so the sent message is returned.
     *
     * @param message The {@link PubSubMessage} to be sent.
     * @return The sent {@link PubSubMessage}.
     * @throws PubSubException if the messaging system throws an error.
     */
    PubSubMessage send(PubSubMessage message) throws PubSubException;
}
