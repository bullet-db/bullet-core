/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.querying.aggregations.RawStrategy;
import com.yahoo.bullet.common.BulletConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

public class RawTest {
    @Test
    public void testRawAggregation() {
        Aggregation aggregation = new Raw(null);
        aggregation.configure(new BulletConfig());

        Assert.assertEquals(aggregation.getType(), AggregationType.RAW);
        Assert.assertEquals(aggregation.getFields(), Collections.emptyList());
        Assert.assertTrue(aggregation.getStrategy(new BulletConfig()) instanceof RawStrategy);
    }

    @Test
    public void testSize() {
        Aggregation aggregation;
        BulletConfig config = new BulletConfig();

        aggregation = new Raw(null);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) BulletConfig.DEFAULT_AGGREGATION_SIZE);

        aggregation = new Raw(-10);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) BulletConfig.DEFAULT_AGGREGATION_SIZE);

        aggregation = new Raw(0);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) BulletConfig.DEFAULT_AGGREGATION_SIZE);

        aggregation = new Raw(1);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 1);

        aggregation = new Raw(BulletConfig.DEFAULT_AGGREGATION_SIZE);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) BulletConfig.DEFAULT_AGGREGATION_SIZE);

        aggregation = new Raw(BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE + 1);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) BulletConfig.DEFAULT_AGGREGATION_MAX_SIZE);
    }

    @Test
    public void testConfiguredSize() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.AGGREGATION_DEFAULT_SIZE, 10);
        config.set(BulletConfig.AGGREGATION_MAX_SIZE, 200);
        // Need to lower or else AGGREGATION_MAX_SIZE will default to DEFAULT_AGGREGATION_MAX_SIZE
        config.set(BulletConfig.GROUP_AGGREGATION_MAX_SIZE, 200);
        config.validate();

        Aggregation aggregation;

        aggregation = new Raw(null);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);

        aggregation = new Raw(-10);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);

        aggregation = new Raw(0);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);

        aggregation = new Raw(1);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 1);

        aggregation = new Raw(10);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 10);

        aggregation = new Raw(2000);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 200);

        aggregation = new Raw(4000);
        aggregation.configure(config);
        Assert.assertEquals(aggregation.getSize(), (Integer) 200);
    }

    @Test
    public void testToString() {
        Aggregation aggregation = new Raw(null);
        aggregation.configure(new BulletConfig());
        Assert.assertEquals(aggregation.toString(), "{size: 500, type: RAW}");
    }
}
