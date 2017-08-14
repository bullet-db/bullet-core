package com.yahoo.bullet.pubsub;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

public class PubSubExceptionTest {
    @Test
    public void testGetMessage() {
        String randomMessage = UUID.randomUUID().toString();
        PubSubException ex = new PubSubException(randomMessage);
        Assert.assertTrue(ex.getMessage().equals(randomMessage));
    }
}
