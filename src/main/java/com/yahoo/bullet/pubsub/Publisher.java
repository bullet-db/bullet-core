package com.yahoo.bullet.pubsub;

public interface Publisher {
    /**
     * Send a message with an ID and content.
     *
     * @param id The ID associated with the message.
     * @param content The content of the message.
     * @throws {@link PubSubException} if the messaging system throws an error.
     */
    default void send(String id, String content) throws PubSubException {
        send(new PubSubMessage(id, content));
    }

    /**
     * Sends a {@link PubSubMessage}. Messages with the same ID should be received in order.
     *
     * @param message The {@link PubSubMessage} to be sent.
     * @throws {@link PubSubException} if the messaging system throws an error.
     */
    void send(PubSubMessage message) throws PubSubException;

    /**
     * Close Publisher and delete all related context.
     */
    void close();
}
