/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query;


import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.query.aggregations.GroupAll;
import com.yahoo.bullet.query.aggregations.Raw;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

public class QueryTest {
    @Test
    public void testDefaults() {
        Query query = new Query(new Projection(), null, new Raw(null), null, new Window(), null);
        BulletConfig config = new BulletConfig();
        query.configure(config);

        Assert.assertEquals(query.getDuration(), (Long) BulletConfig.DEFAULT_QUERY_DURATION);
        Assert.assertEquals(query.getAggregation().getSize(), (Integer) BulletConfig.DEFAULT_AGGREGATION_SIZE);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMissingProjection() {
        new Query(null, null, new Raw(null), null, new Window(), null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMissingAggregation() {
        new Query(new Projection(), null, null, null, new Window(), null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testMissingWindow() {
        new Query(new Projection(), null, new Raw(null), null, null, null);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "Only RAW aggregation type can have window emit type RECORD\\.")
    public void testValidateWindowOnlyRawRecord() {
        new Query(new Projection(), null, new GroupAll(Collections.emptySet()), null, new Window(1, Window.Unit.RECORD), null);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "RAW aggregation type cannot have window include type ALL\\.")
    public void testValidateWindowNoRawAll() {
        new Query(new Projection(), null, new Raw(null), null, new Window(1, Window.Unit.TIME, Window.Unit.ALL, null), null);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "Post query cannot have a window\\.")
    public void testValidatePostQueryNoWindow() {
        Query postQuery = new Query(new Projection(), null, new Raw(null), null, new Window(1, Window.Unit.RECORD), null);
        new Query(null, new Projection(), null, new Raw(null), null, postQuery, new Window(), null);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "Post query cannot have a post query\\.")
    public void testValidatePostQueryNoNestedPostQuery() {
        Query nestedPostQuery = new Query(new Projection(), null, new Raw(null), null, new Window(), null);
        Query postQuery = new Query(null, new Projection(), null, new Raw(null), null, nestedPostQuery, new Window(), null);
        new Query(null, new Projection(), null, new Raw(null), null, postQuery, new Window(), null);
    }

    @Test
    public void testDuration() {
        BulletConfig config = new BulletConfig();

        Query query = new Query(new Projection(), null, new Raw(null), null, new Window(), null);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) BulletConfig.DEFAULT_QUERY_DURATION);

        query = new Query(new Projection(), null, new Raw(null), null, new Window(), -1000L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) BulletConfig.DEFAULT_QUERY_DURATION);

        query = new Query(new Projection(), null, new Raw(null), null, new Window(), 0L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) BulletConfig.DEFAULT_QUERY_DURATION);

        query = new Query(new Projection(), null, new Raw(null), null, new Window(), 1L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 1L);

        query = new Query(new Projection(), null, new Raw(null), null, new Window(), BulletConfig.DEFAULT_QUERY_DURATION);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) BulletConfig.DEFAULT_QUERY_DURATION);

        query = new Query(new Projection(), null, new Raw(null), null, new Window(), BulletConfig.DEFAULT_QUERY_MAX_DURATION);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) BulletConfig.DEFAULT_QUERY_MAX_DURATION);

        // Overflow
        query = new Query(new Projection(), null, new Raw(null), null, new Window(), BulletConfig.DEFAULT_QUERY_MAX_DURATION * 2L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) BulletConfig.DEFAULT_QUERY_MAX_DURATION);
    }

    @Test
    public void testCustomDuration() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.QUERY_DEFAULT_DURATION, 200);
        config.set(BulletConfig.QUERY_MAX_DURATION, 1000);
        config.validate();

        Query query = new Query(new Projection(), null, new Raw(null), null, new Window(), null);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 200L);

        query = new Query(new Projection(), null, new Raw(null), null, new Window(), -1000L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 200L);

        query = new Query(new Projection(), null, new Raw(null), null, new Window(), 0L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 200L);

        query = new Query(new Projection(), null, new Raw(null), null, new Window(), 1L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 1L);

        query = new Query(new Projection(), null, new Raw(null), null, new Window(), 200L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 200L);

        query = new Query(new Projection(), null, new Raw(null), null, new Window(), 1000L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 1000L);

        query = new Query(new Projection(), null, new Raw(null), null, new Window(), 2000L);
        query.configure(config);
        Assert.assertEquals(query.getDuration(), (Long) 1000L);
    }

    @Test
    public void testWindowing() {
        BulletConfig config = new BulletConfig();
        Query query = new Query(new Projection(), null, new Raw(null), null, WindowUtils.makeTumblingWindow(1), null);
        query.configure(config);
        Assert.assertEquals(query.getWindow().getEmitEvery(), (Integer) BulletConfig.DEFAULT_WINDOW_MIN_EMIT_EVERY);
    }

    @Test
    public void testDisableWindowing() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.WINDOW_DISABLE, true);
        config.validate();

        Query query = new Query(new Projection(), null, new Raw(null), null, WindowUtils.makeSlidingWindow(1), null);
        query.configure(config);

        Assert.assertNull(query.getWindow().getEmitEvery());
        Assert.assertNull(query.getWindow().getEmitType());
        Assert.assertNull(query.getWindow().getIncludeType());
        Assert.assertNull(query.getWindow().getIncludeFirst());
    }

    @Test
    public void testToString() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.AGGREGATION_DEFAULT_SIZE, 1);
        config.set(BulletConfig.QUERY_DEFAULT_DURATION, 30000L);
        Query query = new Query(new Projection(), null, new Raw(null), null, new Window(), null);
        query.configure(config.validate());
        Assert.assertEquals(query.toString(), "{tableFunction: null, projection: {fields: null, type: PASS_THROUGH}, filter: null, " +
                                              "aggregation: {size: 1, type: RAW}, postAggregations: null, " +
                                              "window: {emitEvery: null, emitType: null, includeType: null, includeFirst: null}, " +
                                              "duration: 30000, postQuery: null}");
    }
}
