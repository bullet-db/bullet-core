/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

public interface Subscriber extends AutoCloseable {
    /**
     * Gets a new {@link PubSubMessage} from the assigned partition/partitions (Here a partition is a unit of
     * parallelism in the Pub/Sub queue, See {@link PubSub}).
     *
     * @return the received {@link PubSubMessage}.
     * @throws PubSubException when a receive fails.
     */
    PubSubMessage receive() throws PubSubException;

    /**
     * Commits allow clients to implement at least once, at most once or exactly once semantics when processing messages.
     *
     * Common implementations might
     *  1. Ack all received messages.
     *  2. Commit current read offset to persistent/fault tolerant storage.
     *
     * @param id The ID of the message to be marked as committed.
     */
    void commit(String id);

    /**
     * Marks the processing of the {@link PubSubMessage} with the given id as failed.
     *
     * @param id The ID of the PubSubMessage to mark as a processing failure.
     */
    void fail(String id);
}
