/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.pubsub;

import org.testng.Assert;
import org.testng.annotations.Test;

public class PubSubExceptionTest {
    @Test
    public void testGetMessage() {
        String randomMessage = "foo";
        PubSubException ex = new PubSubException(randomMessage);
        Assert.assertTrue(ex.getMessage().equals(randomMessage));
    }

    @Test
    public void testGetArgumentFailedWithoutCause() {
        PubSubException ex = PubSubException.forArgument("bar", null);
        Assert.assertEquals(ex.getMessage(), "Could not read required argument: bar");
        Assert.assertNull(ex.getCause());
    }

    @Test
    public void testGetArgumentFailedWithCause() {
        Throwable cause = new NullPointerException();
        PubSubException ex = PubSubException.forArgument("bar", cause);
        Assert.assertEquals(ex.getMessage(), "Could not read required argument: bar");
        Assert.assertEquals(ex.getCause(), cause);
    }
}
