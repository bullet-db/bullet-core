package com.yahoo.bullet.pubsub;

public interface Publisher {
    /**
     * Sends a {@link PubSubMessage}. Messages with the same ID should be received in order.
     *
     * @param message the {@link PubSubMessage} to be sent.
     * @throws PubSubException if the messaging system throws an error.
     */
    void send(PubSubMessage message) throws PubSubException;

    /**
     * Close Publisher and delete all related context.
     */
    void close();
}
