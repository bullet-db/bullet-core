/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
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

public class ReactiveTest {
    private static List<Map.Entry<Meta.Concept, String>> ALL_METADATA =
        asList(Pair.of(Meta.Concept.WINDOW_METADATA, "window_stats"),
               Pair.of(Meta.Concept.WINDOW_NAME, "name"),
               Pair.of(Meta.Concept.WINDOW_NUMBER, "num"),
               Pair.of(Meta.Concept.WINDOW_SIZE, "size"),
               Pair.of(Meta.Concept.WINDOW_EMIT_TIME, "close"));

    private BulletConfig config;
    private MockStrategy strategy;

    private Window makeReactiveWindow() {
        Window window = WindowUtils.makeReactiveWindow();
        window.configure(config);
        window.initialize();
        return window;
    }

    @BeforeMethod
    private void setup() {
        strategy = new MockStrategy();
        config = new BulletConfig();
        addMetadata(config, ALL_METADATA);
    }

    @Test
    public void testCreation() {
        Window window = makeReactiveWindow();
        Reactive reactive = new Reactive(strategy, window, config);
        Assert.assertFalse(reactive.initialize().isPresent());
        Assert.assertFalse(reactive.isClosed());
        Assert.assertFalse(reactive.isClosedForPartition());
    }

    @Test
    public void testBadMaxWindowSize() {
        Window window = WindowUtils.makeWindow(Window.Unit.RECORD, 10);
        window.configure(config);
        Assert.assertFalse(window.initialize().isPresent());
        Reactive reactive = new Reactive(strategy, window, config);


        Optional<List<BulletError>> errors = reactive.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Reactive.ONLY_ONE_RECORD));

        window = WindowUtils.makeWindow(Window.Unit.TIME, 10000);
        window.configure(config);
        Assert.assertFalse(window.initialize().isPresent());
        reactive = new Reactive(strategy, window, config);

        errors = reactive.initialize();
        Assert.assertTrue(errors.isPresent());
        Assert.assertEquals(errors.get(), singletonList(Reactive.NOT_RECORD));
    }

    @Test
    public void testClosedOnStrategyClosed() {
        Window window = makeReactiveWindow();
        ClosableStrategy strategy = new ClosableStrategy();
        SlidingRecord sliding = new SlidingRecord(strategy, window, config);
        Assert.assertFalse(sliding.initialize().isPresent());

        Assert.assertFalse(sliding.isClosed());
        Assert.assertFalse(sliding.isClosedForPartition());
        strategy.setClosed(true);
        Assert.assertTrue(sliding.isClosed());
        Assert.assertTrue(sliding.isClosedForPartition());
    }

    @Test
    public void testReachingWindowSizeOnConsume() {
        Window window = makeReactiveWindow();
        Reactive reactive = new Reactive(strategy, window, config);
        Assert.assertFalse(reactive.initialize().isPresent());

        Assert.assertFalse(reactive.isClosed());
        Assert.assertFalse(reactive.isClosedForPartition());
        Assert.assertEquals(strategy.getConsumeCalls(), 0);

        reactive.consume(RecordBox.get().getRecord());
        Assert.assertTrue(reactive.isClosed());
        Assert.assertTrue(reactive.isClosedForPartition());
        Assert.assertEquals(strategy.getConsumeCalls(), 1);
    }

    @Test
    public void testReachingWindowSizeOnCombine() {
        Window window = makeReactiveWindow();
        Reactive reactive = new Reactive(strategy, window, config);
        Assert.assertFalse(reactive.initialize().isPresent());

        reactive.consume(RecordBox.get().getRecord());
        Assert.assertTrue(reactive.isClosed());
        Assert.assertTrue(reactive.isClosedForPartition());
        Assert.assertEquals(strategy.getConsumeCalls(), 1);

        byte[] data = reactive.getData();

        // Recreate
        strategy = new MockStrategy();
        reactive = new Reactive(strategy, window, config);
        Assert.assertFalse(reactive.initialize().isPresent());
        Assert.assertFalse(reactive.isClosed());
        Assert.assertFalse(reactive.isClosedForPartition());

        reactive.combine(data);
        Assert.assertTrue(reactive.isClosed());
        Assert.assertTrue(reactive.isClosedForPartition());
        Assert.assertEquals(strategy.getConsumeCalls(), 0);
        Assert.assertEquals(strategy.getCombineCalls(), 1);
    }

    @Test
    public void testResetting() {
        Window window = makeReactiveWindow();
        Reactive reactive = new Reactive(strategy, window, config);
        Assert.assertFalse(reactive.initialize().isPresent());
        Assert.assertEquals(strategy.getResetCalls(), 0);

        reactive.consume(RecordBox.get().getRecord());
        Assert.assertTrue(reactive.isClosed());
        Assert.assertTrue(reactive.isClosedForPartition());
        Assert.assertEquals(strategy.getConsumeCalls(), 1);

        reactive.reset();
        Assert.assertFalse(reactive.isClosed());
        Assert.assertFalse(reactive.isClosedForPartition());
        Assert.assertEquals(strategy.getResetCalls(), 1);
    }

    @Test
    public void testMetadata() {
        Window window = makeReactiveWindow();
        Reactive reactive = new Reactive(strategy, window, config);
        Assert.assertFalse(reactive.initialize().isPresent());

        Assert.assertFalse(reactive.isClosed());
        Assert.assertFalse(reactive.isClosedForPartition());

        reactive.consume(RecordBox.get().getRecord());
        Assert.assertTrue(reactive.isClosed());
        Assert.assertTrue(reactive.isClosedForPartition());

        Assert.assertEquals(strategy.getMetadataCalls(), 0);

        long timeNow = System.currentTimeMillis();
        Meta meta = reactive.getMetadata();
        Assert.assertNotNull(meta);
        Map<String, Object> asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("num"), 1L);
        Assert.assertEquals(asMap.get("name"), SlidingRecord.NAME);
        Assert.assertEquals(asMap.get("size"), 1);
        Assert.assertTrue(((Long) asMap.get("close")) >= timeNow);
        Assert.assertEquals(strategy.getMetadataCalls(), 1);

        reactive.reset();
        Assert.assertFalse(reactive.isClosed());
        Assert.assertFalse(reactive.isClosedForPartition());

        meta = reactive.getMetadata();
        Assert.assertNotNull(meta);
        asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("num"), 2L);
        Assert.assertEquals(asMap.get("name"), SlidingRecord.NAME);
        Assert.assertEquals(asMap.get("size"), 0);
        Assert.assertTrue(((Long) asMap.get("close")) >= timeNow);
        Assert.assertEquals(strategy.getMetadataCalls(), 2);

        reactive.consume(RecordBox.get().getRecord());
        Assert.assertTrue(reactive.isClosed());
        Assert.assertTrue(reactive.isClosedForPartition());

        meta = reactive.getMetadata();
        Assert.assertNotNull(meta);
        asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("num"), 2L);
        Assert.assertEquals(asMap.get("name"), SlidingRecord.NAME);
        Assert.assertEquals(asMap.get("size"), 1);
        Assert.assertTrue(((Long) asMap.get("close")) >= timeNow);
        Assert.assertEquals(strategy.getMetadataCalls(), 3);
    }
}
