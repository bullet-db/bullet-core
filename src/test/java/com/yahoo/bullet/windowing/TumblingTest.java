/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.aggregations.MockStrategy;
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

public class TumblingTest {
    private static List<Map.Entry<Meta.Concept, String>> ALL_METADATA =
        asList(Pair.of(Meta.Concept.WINDOW_METADATA, "window_stats"),
               Pair.of(Meta.Concept.WINDOW_NAME, "name"),
               Pair.of(Meta.Concept.WINDOW_NUMBER, "num"),
               Pair.of(Meta.Concept.WINDOW_EXPECTED_EMIT_TIME, "should_close"),
               Pair.of(Meta.Concept.WINDOW_EMIT_TIME, "close"));

    private BulletConfig config;
    private MockStrategy strategy;

    private Window makeTumblingWindow(int length) {
        Window window = WindowUtils.makeTumblingWindow(length);
        window.configure(config);
        window.initialize();
        return window;
    }

    private Tumbling make(int length, int minimumWindow) {
        config.set(BulletConfig.WINDOW_MIN_EMIT_EVERY, minimumWindow);
        config.validate();
        Window window = makeTumblingWindow(length);
        return new Tumbling(strategy, window, config);
    }

    @BeforeMethod
    private void setup() {
        strategy = new MockStrategy();
        config = new BulletConfig();
        addMetadata(config, ALL_METADATA);
    }

    @Test
    public void testCreation() {
        Tumbling tumbling = make(1000, 1000);
        Assert.assertFalse(tumbling.initialize().isPresent());
    }

    @Test
    public void testClampingToMinimumEmit() {
        Tumbling tumbling = make(1000, 5000);
        Assert.assertFalse(tumbling.initialize().isPresent());
        // The window is what controls this so Tumbling has 5000 for the window size
        Assert.assertEquals(tumbling.windowLength, 5000L);
    }

    @Test
    public void testNotClosedOnStrategyClosed() {
        Window window = makeTumblingWindow(Integer.MAX_VALUE);
        ClosableStrategy strategy = new ClosableStrategy();
        Tumbling tumbling = new Tumbling(strategy, window, config);
        Assert.assertFalse(tumbling.initialize().isPresent());

        Assert.assertFalse(tumbling.isClosed());
        Assert.assertFalse(tumbling.isClosedForPartition());
        strategy.setClosed(true);
        Assert.assertFalse(tumbling.isClosed());
        Assert.assertFalse(tumbling.isClosedForPartition());
    }

    @Test
    public void testWindowIsOpenIfWithinWindowLength() {
        Tumbling tumbling = make(Integer.MAX_VALUE, 1000);
        Assert.assertFalse(tumbling.initialize().isPresent());
        Assert.assertFalse(tumbling.isClosed());
        Assert.assertFalse(tumbling.isClosedForPartition());
    }

    @Test
    public void testClosedWindowAfterTime() throws Exception {
        // Change minimum length to 1 ms
        Tumbling tumbling = make(1, 1);
        Assert.assertFalse(tumbling.initialize().isPresent());

        // Sleep to make sure it's 1 ms
        Thread.sleep(1);

        Assert.assertTrue(tumbling.isClosed());
        Assert.assertTrue(tumbling.isClosedForPartition());
    }

    @Test
    public void testResetting() throws Exception {
        long started = System.currentTimeMillis();
        // Change minimum length to 1 ms
        Tumbling tumbling = make(1, 1);
        Assert.assertEquals(strategy.getResetCalls(), 0);
        Assert.assertFalse(tumbling.initialize().isPresent());
        long originalCloseTime = tumbling.nextCloseTime;
        Assert.assertTrue(originalCloseTime >= started + 1);

        // Sleep to make sure it's 1 ms
        Thread.sleep(1);

        Assert.assertTrue(tumbling.isClosed());
        Assert.assertTrue(tumbling.isClosedForPartition());

        long resetTime = System.currentTimeMillis();
        tumbling.reset();
        long newCloseTime = tumbling.nextCloseTime;
        Assert.assertTrue(resetTime > started);
        Assert.assertTrue(newCloseTime >= originalCloseTime + 1);
        Assert.assertEquals(strategy.getResetCalls(), 1);
    }

    @Test
    public void testResettingForPartition() throws Exception {
        long started = System.currentTimeMillis();
        // Change minimum length to 1 ms
        Tumbling tumbling = make(1, 1);
        Assert.assertEquals(strategy.getResetCalls(), 0);
        Assert.assertFalse(tumbling.initialize().isPresent());
        long originalCloseTime = tumbling.nextCloseTime;
        Assert.assertTrue(originalCloseTime >= started + 1);

        // Sleep to make sure it's 1 ms
        Thread.sleep(1);

        Assert.assertTrue(tumbling.isClosed());
        Assert.assertTrue(tumbling.isClosedForPartition());

        long resetTime = System.currentTimeMillis();
        tumbling.resetForPartition();
        long newCloseTime = tumbling.nextCloseTime;
        Assert.assertTrue(resetTime > started);
        Assert.assertTrue(newCloseTime >= originalCloseTime + 1);
        Assert.assertEquals(strategy.getResetCalls(), 1);
    }

    @Test
    public void testMetadata() throws Exception {
        long started = System.currentTimeMillis();

        Tumbling tumbling = make(1, 1);
        Assert.assertFalse(tumbling.initialize().isPresent());

        Thread.sleep(1);

        tumbling.consume(RecordBox.get().getRecord());
        Assert.assertTrue(tumbling.isClosed());
        Assert.assertTrue(tumbling.isClosedForPartition());

        Assert.assertEquals(strategy.getMetadataCalls(), 0);
        Meta meta = tumbling.getMetadata();
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

        tumbling.reset();
        meta = tumbling.getMetadata();
        Assert.assertNotNull(meta);
        asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("num"), 2L);
        Assert.assertEquals(asMap.get("name"), AdditiveTumbling.NAME);
        Assert.assertEquals(strategy.getMetadataCalls(), 2);
    }
}
