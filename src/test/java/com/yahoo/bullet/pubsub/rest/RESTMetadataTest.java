/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.pubsub.rest;

import com.yahoo.bullet.pubsub.Metadata;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;

public class RESTMetadataTest {
    @Test
    public void testCreationWithoutMetadata() {
        RESTMetadata metadata = new RESTMetadata("foo");
        Assert.assertNull(metadata.getContent());
        Assert.assertNull(metadata.getSignal());
        Assert.assertEquals(metadata.getUrl(), "foo");
    }

    @Test
    public void testCreationWithMetadata() {
        RESTMetadata metadata = new RESTMetadata("foo", new Metadata(Metadata.Signal.CUSTOM, new HashMap<>()));
        Assert.assertEquals(metadata.getSignal(), Metadata.Signal.CUSTOM);
        Assert.assertEquals(metadata.getContent(), Collections.emptyMap());
        Assert.assertEquals(metadata.getUrl(), "foo");
    }

    @Test
    public void testCopy() {
        RESTMetadata metadata = new RESTMetadata("foo", new Metadata(Metadata.Signal.CUSTOM, new HashMap<>()));
        RESTMetadata copy = (RESTMetadata) metadata.copy();
        Assert.assertNotEquals(metadata, copy);
        Assert.assertEquals(metadata.getSignal(), copy.getSignal());
        Assert.assertEquals(metadata.getContent(), copy.getContent());
        Assert.assertEquals(metadata.getUrl(), copy.getUrl());
    }
}
