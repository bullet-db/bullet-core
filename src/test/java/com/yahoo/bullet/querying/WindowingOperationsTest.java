/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.parsing.Aggregation;
import com.yahoo.bullet.parsing.Query;
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.parsing.WindowUtils;
import com.yahoo.bullet.windowing.AdditiveTumbling;
import com.yahoo.bullet.windowing.Basic;
import com.yahoo.bullet.windowing.Reactive;
import com.yahoo.bullet.windowing.Tumbling;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WindowingOperationsTest {
    @Test
    public void testNoWindow() {
        BulletConfig config = new BulletConfig();
        Query query = new Query();
        Assert.assertEquals(WindowingOperations.findScheme(query, null, config).getClass(), Basic.class);
    }

    @Test
    public void testRawReactiveWindow() {
        BulletConfig config = new BulletConfig();
        Query query = new Query();
        Window window = WindowUtils.makeReactiveWindow();
        window.configure(config);
        query.setWindow(window);
        Aggregation aggregation = new Aggregation();
        aggregation.setType(Aggregation.Type.RAW);
        query.setAggregation(aggregation);

        Assert.assertEquals(WindowingOperations.findScheme(query, null, config).getClass(), Reactive.class);
    }

    @Test
    public void testAdditiveTumblingWindow() {
        BulletConfig config = new BulletConfig();
        Query query = new Query();
        Window window = WindowUtils.makeWindow(Window.Unit.TIME, 1000, Window.Unit.ALL, null);
        window.configure(config);
        query.setWindow(window);
        Aggregation aggregation = new Aggregation();
        aggregation.setType(Aggregation.Type.GROUP);
        query.setAggregation(aggregation);

        Assert.assertEquals(WindowingOperations.findScheme(query, null, config).getClass(), AdditiveTumbling.class);
    }

    @Test
    public void testNotForcingRawToReactive() {
        BulletConfig config = new BulletConfig();
        Query query = new Query();
        Window window = WindowUtils.makeWindow(Window.Unit.TIME, 1000, Window.Unit.ALL, null);
        window.configure(config);
        query.setWindow(window);
        Aggregation aggregation = new Aggregation();
        aggregation.setType(Aggregation.Type.RAW);
        query.setAggregation(aggregation);

        Assert.assertEquals(WindowingOperations.findScheme(query, null, config).getClass(), AdditiveTumbling.class);
    }

    @Test
    public void testNotForcingNonRawToTumbling() {
        BulletConfig config = new BulletConfig();
        Query query = new Query();
        Window window = WindowUtils.makeReactiveWindow();
        window.configure(config);
        query.setWindow(window);
        Aggregation aggregation = new Aggregation();
        aggregation.setType(Aggregation.Type.GROUP);
        query.setAggregation(aggregation);

        Assert.assertEquals(WindowingOperations.findScheme(query, null, config).getClass(), Reactive.class);
    }

    @Test
    public void testOtherWindowsForcedToTumbling() {
        BulletConfig config = new BulletConfig();
        Query query = new Query();
        Window window = WindowUtils.makeWindow(Window.Unit.TIME, 1000, Window.Unit.RECORD, 4);
        window.configure(config);
        query.setWindow(window);
        Aggregation aggregation = new Aggregation();
        aggregation.setType(Aggregation.Type.COUNT_DISTINCT);
        query.setAggregation(aggregation);

        Assert.assertEquals(WindowingOperations.findScheme(query, null, config).getClass(), Tumbling.class);
    }

    @Test
    public void testTumblingWindow() {
        BulletConfig config = new BulletConfig();
        Query query = new Query();
        Window window = WindowUtils.makeTumblingWindow(1000);
        window.configure(config);
        query.setWindow(window);

        Assert.assertEquals(WindowingOperations.findScheme(query, null, config).getClass(), Tumbling.class);
    }
}
