/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This implements a {@link Subscriber} that provides a base subscriber that buffers a fixed number of messages read.
 * See {@link BufferingSubscriber#maxUncommitedMessages}.
 *
 * It provides implementations of {@link Subscriber#commit(String, int)}, {@link Subscriber#fail(String, int)} and
 * {@link Subscriber#receive()} that honor the fixed number of messages to read.
 *
 * This class is intended to be used if your PubSub implementation does not care about (or cannot be) using commit and
 * fail to reprocess messages from the PubSub and prefers to manage it in code.
 */
@Slf4j
public abstract class BufferingSubscriber implements Subscriber {
    /**
     * The maximum number of PubSubMessages we can have unacked at any time. Further calls to receive will return nothing.
     */
    protected final int maxUncommitedMessages;

    /**
     * A List of messages read. {@link #receive()} emits from the head.
     */
    protected List<PubSubMessage> receivedMessages = new LinkedList<>();

    /**
     * A Map of messages that have not been committed so far.
     */
    protected Map<Pair<String, Integer>, PubSubMessage> unCommittedMessages = new HashMap<>();

    /**
     * Creates an instance of this class with the given max for commited messages.
     *
     * @param maxUncommitedMessages The maximum number of messages that this Subscriber will buffer.
     */
    public BufferingSubscriber(int maxUncommitedMessages) {
        this.maxUncommitedMessages = maxUncommitedMessages;
    }

    @Override
    public PubSubMessage receive() throws PubSubException {
        if (unCommittedMessages.size() >= maxUncommitedMessages) {
            log.warn("Reached limit of max uncommitted messages: {}. Waiting for commits to proceed.", maxUncommitedMessages);
            return null;
        }
        if (!haveMessages()) {
            return null;
        }
        PubSubMessage message = receivedMessages.remove(0);
        unCommittedMessages.put(ImmutablePair.of(message.getId(), message.getSequence()), message);
        return message;
    }

    /**
     * {@inheritDoc}
     *
     * Marks a message as fully processed. This message is forgotten and cannot be failed after. If we have equal or
     * more than {@link #maxUncommitedMessages} uncommited messages, further calls to {@link #receive()} will return
     * nulls till some messages are commited.
     */
    @Override
    public void commit(String id, int sequence) {
        ImmutablePair<String, Integer> key = ImmutablePair.of(id, sequence);
        unCommittedMessages.remove(key);
    }

    /**
     * {@inheritDoc}
     *
     * Marks a message denoted by the id and sequence to have failed. This message is added for emission and will be
     * emitted on the next {@link BufferingSubscriber#receive()}.
     */
    @Override
    public void fail(String id, int sequence) {
        Pair<String, Integer> compositeID = ImmutablePair.of(id, sequence);
        PubSubMessage message = unCommittedMessages.get(compositeID);
        if (message != null) {
            receivedMessages.add(0, message);
            unCommittedMessages.remove(compositeID);
        }
    }

    /**
     * Returns true if we already have messages to emit or if {@link BufferingSubscriber#getMessages()} returns non-null
     * and non-empty {@link List} of messages. Otherwise, returns false.
     *
     * @return A boolean denoting whether we do have messages to emit.
     * @throws PubSubException if there was an issue reading the messages.
     */
    protected boolean haveMessages() throws PubSubException {
        if (!receivedMessages.isEmpty()) {
            return true;
        }
        List<PubSubMessage> messages = getMessages();
        if (messages == null || messages.isEmpty()) {
            return false;
        }
        receivedMessages.addAll(messages);
        return true;
    }

    /**
     * Implement this method to read and return a {@link List} of {@link PubSubMessage} from your actual PubSub source.
     *
     * @return A {@link List} of {@link PubSubMessage} if any were read. A null or an empty list if not.
     * @throws PubSubException if there was an issue reading the messages.
     */
    protected abstract List<PubSubMessage> getMessages() throws PubSubException;
}
