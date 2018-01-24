/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.parsing.Window;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.parsing.QueryUtils.makeAggregationQuery;

public class RunningQueryTest {
    @Test
    public void testCreatingWithStringQuery() {
        BulletConfig config = new BulletConfig();
        RunningQuery runningQuery = new RunningQuery("foo", "{}", config);
        Assert.assertFalse(runningQuery.initialize().isPresent());
        Assert.assertEquals(runningQuery.getId(), "foo");
        Assert.assertNotNull(runningQuery.getQuery());
        Assert.assertEquals(runningQuery.toString(), "foo : {}");
    }

    @Test
    public void testCreatingWithQuery() {
        BulletConfig config = new BulletConfig();
        Query query = new Query();
        query.setAggregation(new Aggregation());
        query.configure(config);

        Assert.assertFalse(query.initialize().isPresent());
        RunningQuery runningQuery = new RunningQuery("foo", query);
        Assert.assertFalse(runningQuery.initialize().isPresent());
        Assert.assertEquals(runningQuery.getId(), "foo");
        Assert.assertNotNull(runningQuery.getQuery());
        String actual = runningQuery.toString();
        Assert.assertTrue(actual.contains("foo : {"));
        Assert.assertTrue(actual.contains("filters: null, projection: null,"));
        Assert.assertTrue(actual.contains("aggregation: {size: 500, type: RAW, fields: null, attributes: null},"));
        Assert.assertTrue(actual.contains("window: null, duration:"));
        Assert.assertTrue(actual.contains("}"));
    }

    @Test
    public void testInitialization() {
        BulletConfig config = new BulletConfig();
        String query = makeAggregationQuery(Aggregation.Type.RAW, null, Window.Unit.RECORD, 10, Window.Unit.TIME, 2000);
        RunningQuery runningQuery = new RunningQuery("foo", query, config);

        Optional<List<BulletError>> errors = runningQuery.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get().size(), 1);
        Assert.assertEquals(errors.get().get(0), Window.IMPROPER_INCLUDE);
    }

    @Test
    public void testStartTime() {
        long start = System.currentTimeMillis();
        BulletConfig config = new BulletConfig();
        RunningQuery runningQuery = new RunningQuery("foo", "{}", config);
        Assert.assertFalse(runningQuery.initialize().isPresent());
        long end = System.currentTimeMillis();
        Assert.assertTrue(runningQuery.getStartTime() >= start);
        Assert.assertTrue(runningQuery.getStartTime() <= end);
    }
}
