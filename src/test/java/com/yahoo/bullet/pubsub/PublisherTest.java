/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

public class PublisherTest {
    private class MockPublisher implements Publisher {
        PubSubMessage sentMessage;

        @Override
        public void send(PubSubMessage message) {
            sentMessage = message;
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    public void testDefaultSend() throws PubSubException {
        String randomId = UUID.randomUUID().toString();
        String randomMessage = UUID.randomUUID().toString();
        MockPublisher mockPublisher = new MockPublisher();
        mockPublisher.send(randomId, randomMessage);

        Assert.assertTrue(mockPublisher.sentMessage.getContent().equals(randomMessage));
        Assert.assertTrue(mockPublisher.sentMessage.getId().equals(randomId));
    }
}
