/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub;

import com.yahoo.bullet.pubsub.Metadata.Signal;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetadataTest {
    @Test
    public void testHasSignal() {
        Metadata full = new Metadata(Signal.ACKNOWLEDGE, 5);
        Metadata empty = new Metadata();
        Assert.assertTrue(full.hasSignal());
        Assert.assertFalse(empty.hasSignal());
    }

    @Test
    public void testSignalTypes() {
        Metadata empty = new Metadata();
        Assert.assertFalse(empty.hasSignal());
        Assert.assertTrue(new Metadata(Signal.ACKNOWLEDGE, null).hasSignal(Signal.ACKNOWLEDGE));
        Assert.assertTrue(new Metadata(Signal.FAIL, null).hasSignal(Signal.FAIL));
        Assert.assertTrue(new Metadata(Signal.COMPLETE, null).hasSignal(Signal.COMPLETE));
        Assert.assertFalse(new Metadata(Signal.ACKNOWLEDGE, null).hasSignal(Signal.FAIL));
    }

    @Test
    public void testHasContent() {
        Metadata full = new Metadata(Signal.ACKNOWLEDGE, 5);
        Metadata empty = new Metadata();
        Assert.assertTrue(full.hasContent());
        Assert.assertFalse(empty.hasContent());
    }

    @Test
    public void testCreatedTimestamp() {
        long before = System.currentTimeMillis();
        Metadata metadata = new Metadata();
        long after = System.currentTimeMillis();
        Assert.assertTrue(before <= metadata.getCreated());
        Assert.assertTrue(after >= metadata.getCreated());
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
        empty.setSignal(Signal.ACKNOWLEDGE);
        Assert.assertEquals(empty.getSignal(), Signal.ACKNOWLEDGE);
    }

    @Test
    public void testSetCreated() {
        Metadata metadata = new Metadata();
        Assert.assertNotEquals(metadata.getCreated(), 0L);

        metadata.setCreated(0L);
        Assert.assertEquals(metadata.getCreated(), 0L);
    }
}
