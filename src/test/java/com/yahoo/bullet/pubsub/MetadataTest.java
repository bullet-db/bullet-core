/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MetadataTest {
    @Test
    public void testHasSignal() {
        Metadata full = new Metadata(Metadata.Signal.ACKNOWLEDGE, 5);
        Metadata empty = new Metadata();
        Assert.assertTrue(full.hasSignal());
        Assert.assertFalse(empty.hasSignal());
    }

    @Test
    public void testHasContent() {
        Metadata full = new Metadata(Metadata.Signal.ACKNOWLEDGE, 5);
        Metadata empty = new Metadata();
        Assert.assertTrue(full.hasContent());
        Assert.assertFalse(empty.hasContent());
    }

    @Test
    public void testSetContentWhenEmpty() {
        Metadata empty = new Metadata();
        empty.setContent(5);
        Assert.assertEquals(empty.getContent(), 5);
    }

    @Test
    public void testSetSignalWhenEmpty() {
        Metadata empty = new Metadata();
        empty.setSignal(Metadata.Signal.ACKNOWLEDGE);
        Assert.assertEquals(empty.getSignal(), Metadata.Signal.ACKNOWLEDGE);
    }
}
