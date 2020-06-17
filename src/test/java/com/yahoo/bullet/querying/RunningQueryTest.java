/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.pubsub.Metadata;
import com.yahoo.bullet.query.Projection;
import com.yahoo.bullet.query.Query;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.aggregations.Raw;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RunningQueryTest {
    @Test
    public void testCreatingWithQuery() {
        BulletConfig config = new BulletConfig();
        Query query = new Query(new Projection(), null, new Raw(null), null, new Window(), null);
        query.configure(config);

        RunningQuery runningQuery = new RunningQuery("foo", query, new Metadata(null, "bar"));
        Assert.assertEquals(runningQuery.getId(), "foo");
        Assert.assertNotNull(runningQuery.getQuery());
        Assert.assertEquals(runningQuery.getQueryString(), "bar");
        Assert.assertEquals(runningQuery.toString(), query.toString());
    }

    @Test
    public void testStartTime() {
        long start = System.currentTimeMillis();
        BulletConfig config = new BulletConfig();
        Query query = new Query(new Projection(), null, new Raw(null), null, new Window(), null);
        query.configure(config);

        RunningQuery runningQuery = new RunningQuery("foo", query, new Metadata(null, null));

        long end = System.currentTimeMillis();
        Assert.assertTrue(runningQuery.getStartTime() >= start);
        Assert.assertTrue(runningQuery.getStartTime() <= end);
        Assert.assertFalse(runningQuery.isTimedOut());
    }

    @Test
    public void testTimingOut() throws Exception {
        BulletConfig config = new BulletConfig();
        Query query = new Query(new Projection(), null, new Raw(null), null, new Window(), 1L);
        query.configure(config);

        RunningQuery runningQuery = new RunningQuery("foo", query, new Metadata(null, null));

        // Sleep to make sure it's 1 ms
        Thread.sleep(1);

        Assert.assertTrue(runningQuery.isTimedOut());
    }
}
