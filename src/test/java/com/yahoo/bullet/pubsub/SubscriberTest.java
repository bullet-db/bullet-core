/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import lombok.NoArgsConstructor;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

public class SubscriberTest {
    @NoArgsConstructor
    private static class MockSubscriber implements Subscriber {
        private String commitID;
        private String failID;

        public PubSubMessage receive() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void commit(String id) {
            commitID = id;
        }

        @Override
        public void fail(String id) {
            failID = id;
        }

        public void close() {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    public void testCommitWithNoSequenceNumber() {
        String randomID = UUID.randomUUID().toString();
        MockSubscriber subscriber = new MockSubscriber();
        subscriber.commit(randomID);
        Assert.assertEquals(randomID, subscriber.commitID);
    }

    @Test
    public void testFailWithNoSequenceNumber() {
        String randomID = UUID.randomUUID().toString();
        MockSubscriber subscriber = new MockSubscriber();
        subscriber.fail(randomID);
        Assert.assertEquals(randomID, subscriber.failID);
    }
}
