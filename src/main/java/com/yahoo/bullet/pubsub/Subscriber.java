package com.yahoo.bullet.pubsub;

public abstract class Subscriber {
    /**
     * Gets a new {@link PubSubMessage} from the assigned partition/partitions (Here a partition is a unit of
     * parallelism in the Pub/Sub queue, See {@link PubSub}).
     *
     * @return the received {@link PubSubMessage}.
     * @throws PubSubException when a receive fails.
     */
    public abstract PubSubMessage receive() throws PubSubException;

    /**
     * Close the Subscriber and delete all associated Context.
     */
    public abstract void close();

    /**
     * Commits allow clients to implement at least once, at most once or exactly once semantics when processing messages.
     *
     * Common implementations might
     *  1. Ack all received messages.
     *  2. Commit current read offset to persistent/fault tolerant storage.
     *
     *  @param id the query ID of the message to be marked as committed.
     *  @param sequenceNumber the sequence number of the message to be committed.
     */
    public abstract void commit(String id, long sequenceNumber);

    /**
     * Convenience method to commit a message that doesn't contain a sequence number.
     *
     * @param id is the ID of the message to be marked as committed.
     */
    public void commit(String id) {
        commit(id, -1);
    }

    /**
     * Marks the processing of the {@link PubSubMessage} with the given id as failed.
     *
     * @param id the ID of the PubSubMessage to mark as a processing failure.
     */
    public abstract void fail(String id, long sequenceNumber);

    /**
     * Convenience method to fail a message that doesn't contain a sequence number.
     *
     * @param id is the ID of the message to be marked as a processing failure.
     */
    public void fail(String id) {
        fail(id, -1);
    }

}
