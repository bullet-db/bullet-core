/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.querying.aggregations.MockStrategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.query.Window;
import com.yahoo.bullet.query.WindowUtils;
import com.yahoo.bullet.result.Meta;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static com.yahoo.bullet.TestHelpers.addMetadata;
import static java.util.Arrays.asList;

public class AdditiveTumblingTest {
    private static List<Map.Entry<Meta.Concept, String>> ALL_METADATA =
        asList(Pair.of(Meta.Concept.WINDOW_METADATA, "window_stats"),
               Pair.of(Meta.Concept.WINDOW_NAME, "name"),
               Pair.of(Meta.Concept.WINDOW_NUMBER, "num"),
               Pair.of(Meta.Concept.WINDOW_EXPECTED_EMIT_TIME, "should_close"),
               Pair.of(Meta.Concept.WINDOW_EMIT_TIME, "close"));

    private BulletConfig config;
    private MockStrategy strategy;

    private Window makeAdditiveTumblingWindow(int length) {
        Window window = WindowUtils.makeWindow(Window.Unit.TIME, length, Window.Unit.ALL, null);
        window.configure(config);
        return window;
    }

    private AdditiveTumbling make(int length, int minimumWindow) {
        config.set(BulletConfig.WINDOW_MIN_EMIT_EVERY, minimumWindow);
        config.validate();
        Window window = makeAdditiveTumblingWindow(length);
        return new AdditiveTumbling(strategy, window, config);
    }

    @BeforeMethod
    private void setup() {
        strategy = new MockStrategy();
        config = new BulletConfig();
        addMetadata(config, ALL_METADATA);
    }

    @Test
    public void testCreation() {
        AdditiveTumbling additiveTumbling = make(2000, 1000);
    }

    @Test
    public void testClampingToMinimumEmit() {
        AdditiveTumbling additiveTumbling = make(1000, 5000);
        // The window is what controls this so AdditiveTumbling has 5000 for the window size
        Assert.assertEquals(additiveTumbling.windowLength, 5000L);
    }

    @Test
    public void testNotClosedOnStrategyClosed() {
        Window window = WindowUtils.makeWindow(Window.Unit.TIME, Integer.MAX_VALUE, Window.Unit.ALL, null);
        ClosableStrategy strategy = new ClosableStrategy();
        AdditiveTumbling additiveTumbling = new AdditiveTumbling(strategy, window, config);

        Assert.assertFalse(additiveTumbling.isClosed());
        Assert.assertFalse(additiveTumbling.isClosedForPartition());
        strategy.setClosed(true);
        Assert.assertFalse(additiveTumbling.isClosed());
        Assert.assertFalse(additiveTumbling.isClosedForPartition());
    }

    @Test
    public void testWindowIsOpenIfWithinWindowLength() {
        AdditiveTumbling additiveTumbling = make(Integer.MAX_VALUE, 1000);
        Assert.assertFalse(additiveTumbling.isClosed());
        Assert.assertFalse(additiveTumbling.isClosedForPartition());
    }

    @Test
    public void testClosedWindowAfterTime() throws Exception {
        // Change minimum length to 1 ms
        AdditiveTumbling additiveTumbling = make(1, 1);

        // Sleep to make sure it's 1 ms
        Thread.sleep(1);

        Assert.assertTrue(additiveTumbling.isClosed());
        Assert.assertTrue(additiveTumbling.isClosedForPartition());
    }

    @Test
    public void testResetting() throws Exception {
        long started = System.currentTimeMillis();
        // Change minimum length to 1 ms
        AdditiveTumbling additiveTumbling = make(1, 1);
        Assert.assertEquals(strategy.getResetCalls(), 0);
        long originalCloseTime = additiveTumbling.nextCloseTime;
        Assert.assertTrue(originalCloseTime >= started + 1);

        // Sleep to make sure it's 1 ms
        Thread.sleep(1);

        Assert.assertTrue(additiveTumbling.isClosed());
        Assert.assertTrue(additiveTumbling.isClosedForPartition());

        long resetTime = System.currentTimeMillis();
        additiveTumbling.reset();
        long newCloseTime = additiveTumbling.nextCloseTime;
        Assert.assertTrue(resetTime > started);
        Assert.assertTrue(newCloseTime >= originalCloseTime + 1);
        // Aggregation should NOT have been reset
        Assert.assertEquals(strategy.getResetCalls(), 0);
    }

    @Test
    public void testResettingInPartitionMode() throws Exception {
        long started = System.currentTimeMillis();
        // Change minimum length to 1 ms
        AdditiveTumbling additiveTumbling = make(1, 1);
        Assert.assertEquals(strategy.getResetCalls(), 0);
        long originalCloseTime = additiveTumbling.nextCloseTime;
        Assert.assertTrue(originalCloseTime >= started + 1);

        // Sleep to make sure it's 1 ms
        Thread.sleep(1);

        Assert.assertTrue(additiveTumbling.isClosed());
        Assert.assertTrue(additiveTumbling.isClosedForPartition());

        long resetTime = System.currentTimeMillis();
        additiveTumbling.resetForPartition();
        long newCloseTime = additiveTumbling.nextCloseTime;
        Assert.assertTrue(resetTime > started);
        Assert.assertTrue(newCloseTime >= originalCloseTime + 1);
        // Aggregation should have been reset
        Assert.assertEquals(strategy.getResetCalls(), 1);
    }

    @Test
    public void testMetadata() throws Exception {
        long started = System.currentTimeMillis();

        AdditiveTumbling additiveTumbling = make(1, 1);

        Thread.sleep(1);

        additiveTumbling.consume(RecordBox.get().getRecord());
        Assert.assertTrue(additiveTumbling.isClosed());
        Assert.assertTrue(additiveTumbling.isClosedForPartition());

        Assert.assertEquals(strategy.getMetadataCalls(), 0);
        Meta meta = additiveTumbling.getMetadata();
        Assert.assertNotNull(meta);
        Map<String, Object> asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("num"), 1L);
        Assert.assertEquals(asMap.get("name"), Tumbling.NAME);
        long closedTime = (Long) asMap.get("close");
        long shouldCloseTime = (Long) asMap.get("should_close");
        Assert.assertTrue(closedTime >= started);
        Assert.assertTrue(shouldCloseTime >= started);
        Assert.assertTrue(shouldCloseTime <= closedTime);
        Assert.assertEquals(strategy.getMetadataCalls(), 1);

        additiveTumbling.reset();
        meta = additiveTumbling.getMetadata();
        Assert.assertNotNull(meta);
        asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("num"), 2L);
        Assert.assertEquals(asMap.get("name"), AdditiveTumbling.NAME);
        Assert.assertEquals(strategy.getMetadataCalls(), 2);
    }
}
