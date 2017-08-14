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
     * Close Publisher and delete all related Context.
     */
    void close();

    /**
     * Commits allow clients to implement at least once, at most once or exactly once semantics when processing messages.
     *
     * Common implementations might flush the message from a local buffer to the Pub/Sub.
     *
     * @param id the ID of the message to be marked as committed.
     */
    void commit(String id);
}
