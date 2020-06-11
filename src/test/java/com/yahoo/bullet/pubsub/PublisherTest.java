/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.common.SerializerDeserializer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

public class PublisherTest {
    private static class MockPublisher implements Publisher {
        PubSubMessage sentMessage;

        @Override
        public PubSubMessage send(PubSubMessage message) {
            sentMessage = message;
            return message;
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    public void testDefaultSend() throws PubSubException {
        String randomId = UUID.randomUUID().toString();
        byte[] randomMessage = SerializerDeserializer.toBytes(UUID.randomUUID());
        MockPublisher mockPublisher = new MockPublisher();
        PubSubMessage message = mockPublisher.send(randomId, randomMessage);

        Assert.assertEquals(message.getContent(), randomMessage);
        Assert.assertEquals(message.getId(), randomId);
        Assert.assertEquals(mockPublisher.sentMessage.getContent(), randomMessage);
        Assert.assertEquals(mockPublisher.sentMessage.getId(), randomId);
    }
}
