/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
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
