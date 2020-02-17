/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.aggregations.MockStrategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Window;
import com.yahoo.bullet.parsing.WindowUtils;
import com.yahoo.bullet.result.Meta;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.yahoo.bullet.TestHelpers.addMetadata;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class SlidingRecordTest {
    private static List<Map.Entry<Meta.Concept, String>> ALL_METADATA =
        asList(Pair.of(Meta.Concept.WINDOW_METADATA, "window_stats"),
               Pair.of(Meta.Concept.WINDOW_NAME, "name"),
               Pair.of(Meta.Concept.WINDOW_NUMBER, "num"),
               Pair.of(Meta.Concept.WINDOW_SIZE, "size"),
               Pair.of(Meta.Concept.WINDOW_EMIT_TIME, "close"));

    private Window makeSlidingWindow(int size) {
        Window window = WindowUtils.makeWindow(Window.Unit.RECORD, size);
        window.configure(config);
        window.initialize();
        return window;
    }

    private BulletConfig config;
    private MockStrategy strategy;

    @BeforeMethod
    private void setup() {
        strategy = new MockStrategy();
        config = new BulletConfig();
        addMetadata(config, ALL_METADATA);
    }

    @Test
    public void testCreation() {
        Window window = makeSlidingWindow(10);
        SlidingRecord sliding = new SlidingRecord(strategy, window, config);
        Assert.assertFalse(sliding.initialize().isPresent());
        Assert.assertFalse(sliding.isClosed());
        Assert.assertFalse(sliding.isClosedForPartition());
    }

    @Test
    public void testImproperInitialization() {
        SlidingRecord sliding = new SlidingRecord(strategy, new Window(), config);

        Optional<List<BulletError>> errors = sliding.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(SlidingRecord.NOT_RECORD));
    }

    @Test
    public void testNotClosedOnStrategyClosed() {
        Window window = makeSlidingWindow(5);
        ClosableStrategy strategy = new ClosableStrategy();
        SlidingRecord sliding = new SlidingRecord(strategy, window, config);
        Assert.assertFalse(sliding.initialize().isPresent());

        Assert.assertFalse(sliding.isClosed());
        Assert.assertFalse(sliding.isClosedForPartition());
        strategy.setClosed(true);
        Assert.assertFalse(sliding.isClosed());
        Assert.assertFalse(sliding.isClosedForPartition());
    }

    @Test
    public void testReachingWindowSizeOnConsume() {
        Window window = makeSlidingWindow(5);
        SlidingRecord sliding = new SlidingRecord(strategy, window, config);
        Assert.assertFalse(sliding.initialize().isPresent());

        Assert.assertFalse(sliding.isClosed());
        Assert.assertFalse(sliding.isClosedForPartition());

        for (int i = 0; i < 4; ++i) {
            sliding.consume(RecordBox.get().getRecord());
            Assert.assertFalse(sliding.isClosed());
            Assert.assertTrue(sliding.isClosedForPartition());
        }
        sliding.consume(RecordBox.get().getRecord());
        Assert.assertTrue(sliding.isClosed());
        Assert.assertTrue(sliding.isClosedForPartition());
        Assert.assertEquals(strategy.getConsumeCalls(), 5);
    }

    @Test
    public void testReachingWindowSizeOnCombine() {
        Window window = makeSlidingWindow(10);
        SlidingRecord sliding = new SlidingRecord(strategy, window, config);
        Assert.assertFalse(sliding.initialize().isPresent());

        Assert.assertFalse(sliding.isClosed());
        Assert.assertFalse(sliding.isClosedForPartition());

        for (int i = 0; i < 6; ++i) {
            sliding.consume(RecordBox.get().getRecord());
            Assert.assertFalse(sliding.isClosed());
            Assert.assertTrue(sliding.isClosedForPartition());
        }
        Assert.assertEquals(strategy.getConsumeCalls(), 6);
        byte[] data = sliding.getData();

        // Remake
        strategy = new MockStrategy();
        sliding = new SlidingRecord(strategy, window, config);
        Assert.assertFalse(sliding.initialize().isPresent());

        // Combine in the old data
        sliding.combine(data);

        for (int i = 0; i < 3; ++i) {
            sliding.consume(RecordBox.get().getRecord());
            Assert.assertFalse(sliding.isClosed());
            Assert.assertTrue(sliding.isClosedForPartition());
        }
        Assert.assertFalse(sliding.isClosed());
        Assert.assertTrue(sliding.isClosedForPartition());

        sliding.consume(RecordBox.get().getRecord());
        Assert.assertTrue(sliding.isClosed());
        Assert.assertTrue(sliding.isClosedForPartition());
        Assert.assertEquals(strategy.getConsumeCalls(), 4);
        Assert.assertEquals(strategy.getCombineCalls(), 1);
    }

    @Test
    public void testResetting() {
        Window window = makeSlidingWindow(5);
        SlidingRecord sliding = new SlidingRecord(strategy, window, config);
        Assert.assertFalse(sliding.initialize().isPresent());
        Assert.assertEquals(strategy.getResetCalls(), 0);

        for (int i = 0; i < 4; ++i) {
            sliding.consume(RecordBox.get().getRecord());
            Assert.assertFalse(sliding.isClosed());
            Assert.assertTrue(sliding.isClosedForPartition());
        }
        Assert.assertEquals(strategy.getConsumeCalls(), 4);

        sliding.consume(RecordBox.get().getRecord());
        Assert.assertTrue(sliding.isClosed());
        Assert.assertTrue(sliding.isClosedForPartition());

        sliding.reset();
        Assert.assertFalse(sliding.isClosed());
        Assert.assertFalse(sliding.isClosedForPartition());

        Assert.assertEquals(strategy.getConsumeCalls(), 5);
        Assert.assertEquals(strategy.getResetCalls(), 1);
    }

    @Test
    public void testResettingForPartition() {
        Window window = makeSlidingWindow(5);
        SlidingRecord sliding = new SlidingRecord(strategy, window, config);
        Assert.assertFalse(sliding.initialize().isPresent());
        Assert.assertEquals(strategy.getResetCalls(), 0);

        for (int i = 0; i < 4; ++i) {
            sliding.consume(RecordBox.get().getRecord());
            Assert.assertFalse(sliding.isClosed());
            Assert.assertTrue(sliding.isClosedForPartition());
        }
        Assert.assertEquals(strategy.getConsumeCalls(), 4);

        sliding.consume(RecordBox.get().getRecord());
        Assert.assertTrue(sliding.isClosed());
        Assert.assertTrue(sliding.isClosedForPartition());

        sliding.resetForPartition();
        Assert.assertFalse(sliding.isClosed());
        Assert.assertFalse(sliding.isClosedForPartition());

        Assert.assertEquals(strategy.getConsumeCalls(), 5);
        Assert.assertEquals(strategy.getResetCalls(), 1);
    }

    @Test
    public void testMetadata() {
        Window window = makeSlidingWindow(5);
        SlidingRecord sliding = new SlidingRecord(strategy, window, config);
        Assert.assertFalse(sliding.initialize().isPresent());

        for (int i = 0; i < 4; ++i) {
            sliding.consume(RecordBox.get().getRecord());
            Assert.assertFalse(sliding.isClosed());
            Assert.assertTrue(sliding.isClosedForPartition());
        }

        Assert.assertEquals(strategy.getMetadataCalls(), 0);

        long timeNow = System.currentTimeMillis();
        Meta meta = sliding.getMetadata();
        Assert.assertNotNull(meta);
        Map<String, Object> asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("num"), 1L);
        Assert.assertEquals(asMap.get("name"), SlidingRecord.NAME);
        Assert.assertEquals(asMap.get("size"), 4);
        Assert.assertTrue(((Long) asMap.get("close")) >= timeNow);
        Assert.assertEquals(strategy.getMetadataCalls(), 1);

        sliding.consume(RecordBox.get().getRecord());
        Assert.assertTrue(sliding.isClosed());
        Assert.assertTrue(sliding.isClosedForPartition());

        meta = sliding.getMetadata();
        Assert.assertNotNull(meta);
        asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("num"), 1L);
        Assert.assertEquals(asMap.get("name"), SlidingRecord.NAME);
        Assert.assertEquals(asMap.get("size"), 5);
        Assert.assertTrue(((Long) asMap.get("close")) >= timeNow);
        Assert.assertEquals(strategy.getMetadataCalls(), 2);

        sliding.reset();
        Assert.assertFalse(sliding.isClosed());
        Assert.assertFalse(sliding.isClosedForPartition());

        meta = sliding.getMetadata();
        Assert.assertNotNull(meta);
        asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("num"), 2L);
        Assert.assertEquals(asMap.get("name"), SlidingRecord.NAME);
        Assert.assertEquals(asMap.get("size"), 0);
        Assert.assertTrue(((Long) asMap.get("close")) >= timeNow);
        Assert.assertEquals(strategy.getMetadataCalls(), 3);

        sliding.consume(RecordBox.get().getRecord());
        Assert.assertFalse(sliding.isClosed());
        Assert.assertTrue(sliding.isClosedForPartition());

        meta = sliding.getMetadata();
        Assert.assertNotNull(meta);
        asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("num"), 2L);
        Assert.assertEquals(asMap.get("name"), SlidingRecord.NAME);
        Assert.assertEquals(asMap.get("size"), 1);
        Assert.assertTrue(((Long) asMap.get("close")) >= timeNow);
        Assert.assertEquals(strategy.getMetadataCalls(), 4);
    }
}
