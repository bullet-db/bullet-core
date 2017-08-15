package com.yahoo.bullet.pubsub;

import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.UUID;

public class SubscriberTest {
    @Test
    public void testCommitWithNoSequenceNumber() {
        String randomID = UUID.randomUUID().toString();
        Subscriber subscriber = Mockito.mock(Subscriber.class, Mockito.CALLS_REAL_METHODS);

        subscriber.commit(randomID);
        Mockito.verify(subscriber).commit(randomID);
        Mockito.verify(subscriber).commit(randomID, -1);
    }

    @Test
    public void testFailWithNoSequenceNumber() {
        String randomID = UUID.randomUUID().toString();
        Subscriber subscriber = Mockito.mock(Subscriber.class, Mockito.CALLS_REAL_METHODS);

        subscriber.fail(randomID);
        Mockito.verify(subscriber).fail(randomID);
        Mockito.verify(subscriber).fail(randomID, -1);
    }
}
