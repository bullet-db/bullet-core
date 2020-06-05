/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.querying.postaggregations.CullingStrategy;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

public class CullingTest {
    @Test
    public void testCulling() {
        Culling culling = new Culling(Collections.singleton("abc"));

        Assert.assertEquals(culling.getTransientFields(), Collections.singleton("abc"));
        Assert.assertEquals(culling.getType(), PostAggregationType.CULLING);
        Assert.assertEquals(culling.toString(), "{type: CULLING, transientFields: [abc]}");
        Assert.assertTrue(culling.getPostStrategy() instanceof CullingStrategy);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullFields() {
        new Culling(null);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "The CULLING post-aggregation requires at least one field\\.")
    public void testConstructorMissingFields() {
        new Culling(Collections.emptySet());
    }
}
