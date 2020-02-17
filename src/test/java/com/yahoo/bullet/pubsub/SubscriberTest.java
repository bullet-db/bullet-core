/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.pubsub;

import lombok.NoArgsConstructor;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

public class SubscriberTest {
    @NoArgsConstructor
    private class MockSubscriber implements Subscriber {
        String commitID;
        String failID;

        int commitSequence;
        int failSequence;

        public PubSubMessage receive() {
            throw new UnsupportedOperationException();
        }

        public void commit(String id, int sequence) {
            commitID = id;
            commitSequence = sequence;
        }

        public void fail(String id, int sequence) {
            failID = id;
            failSequence = sequence;
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
        Assert.assertEquals(subscriber.commitSequence, -1);
        Assert.assertTrue(subscriber.commitID.equals(randomID));
    }

    @Test
    public void testFailWithNoSequenceNumber() {
        String randomID = UUID.randomUUID().toString();
        MockSubscriber subscriber = new MockSubscriber();
        subscriber.fail(randomID);
        Assert.assertEquals(subscriber.failSequence, -1);
        Assert.assertTrue(subscriber.failID.equals(randomID));
    }
}
