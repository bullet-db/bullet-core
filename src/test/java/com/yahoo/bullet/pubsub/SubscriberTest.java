package com.yahoo.bullet.pubsub;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

public class SubscriberTest {
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
