package com.yahoo.bullet.pubsub;

public interface Subscriber {
    /**
     * Gets a new {@link PubSubMessage} from the assigned partition/partitions (Here a partition is a unit of
     * parallelism in the Pub/Sub queue, See {@link PubSub}).
     *
     * @return the received {@link PubSubMessage}.
     * @throws PubSubException when a receive fails.
     */
    PubSubMessage receive() throws PubSubException;

    /**
     * Close the Subscriber and delete all associated Context.
     */
    void close();

    /**
     * Commits allow clients to implement at least once, at most once or exactly once semantics when processing messages.
     *
     * Common implementations might
     *  1. Ack all received messages.
     *  2. Commit current read offset to persistent/fault tolerant storage.
     *
     * @param id The ID of the message to be marked as committed.
     * @param sequence The sequence number of the message to be committed.
     */
    void commit(String id, int sequence);

    /**
     * Convenience method to commit a message that doesn't contain a sequence number.
     *
     * @param id The ID of the message to be marked as committed.
     */
    default void commit(String id) {
        commit(id, -1);
    }

    /**
     * Marks the processing of the {@link PubSubMessage} with the given id as failed.
     *
     * @param id The ID of the PubSubMessage to mark as a processing failure.
     * @param sequence The sequence number of the PubSubMessage to mark as a processing failure.
     */
    void fail(String id, int sequence);

    /**
     * Convenience method to fail a message that doesn't contain a sequence number.
     *
     * @param id The ID of the message to be marked as a processing failure.
     */
    default void fail(String id) {
        fail(id, -1);
    }

}
