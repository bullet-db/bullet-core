/*
 *  Copyright 2018, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.windowing;

import com.yahoo.bullet.aggregations.MockStrategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Meta;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.yahoo.bullet.TestHelpers.addMetadata;
import static java.util.Arrays.asList;

public class BasicTest {
    private static List<Map.Entry<Meta.Concept, String>> ALL_METADATA =
        asList(Pair.of(Meta.Concept.WINDOW_METADATA, "window_stats"),
               Pair.of(Meta.Concept.WINDOW_NAME, "name"),
               Pair.of(Meta.Concept.WINDOW_NUMBER, "num"),
               Pair.of(Meta.Concept.WINDOW_EMIT_TIME, "closed"));

    private BulletConfig config;
    private MockStrategy strategy;

    @BeforeMethod
    private void setup() {
        config = new BulletConfig();
        strategy = new MockStrategy();
    }

    @Test
    public void testCreation() {
        Basic basic = new Basic(strategy, null, config);
        Assert.assertNotNull(basic.getMetadata());
    }

    @Test
    public void testAllDataMethodsProxyToStrategy() {
        addMetadata(config, ALL_METADATA);
        Basic basic = new Basic(strategy, null, config);

        Assert.assertEquals(strategy.getConsumeCalls(), 0);
        Assert.assertEquals(strategy.getCombineCalls(), 0);
        Assert.assertEquals(strategy.getDataCalls(), 0);
        Assert.assertEquals(strategy.getMetadataCalls(), 0);
        Assert.assertEquals(strategy.getRecordCalls(), 0);
        Assert.assertEquals(strategy.getResetCalls(), 0);

        basic.consume(null);
        Assert.assertEquals(strategy.getConsumeCalls(), 1);

        basic.combine(null);
        Assert.assertEquals(strategy.getCombineCalls(), 1);

        Assert.assertNull(basic.getData());
        Assert.assertEquals(strategy.getDataCalls(), 1);

        long timeNow = System.currentTimeMillis();
        Meta meta = basic.getMetadata();
        Assert.assertNotNull(meta);
        Map<String, Object> asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("name"), Basic.NAME);
        Assert.assertEquals(asMap.get("num"), 1L);
        Assert.assertTrue(((Long) asMap.get("closed")) >= timeNow);
        Assert.assertEquals(strategy.getMetadataCalls(), 1);

        Assert.assertNull(basic.getRecords());
        Assert.assertEquals(strategy.getRecordCalls(), 1);

        Clip clip = basic.getResult();
        Map<String, Object> expected = (Map<String, Object>) meta.asMap().get("window_stats");
        Map<String, Object> actual = (Map<String, Object>) clip.getMeta().asMap().get("window_stats");
        Assert.assertEquals(actual.get("name"), expected.get("name"));
        Assert.assertEquals(actual.get("num"), expected.get("num"));
        Assert.assertTrue(((Long) actual.get("closed")) >= timeNow);

        Assert.assertEquals(clip.getRecords(), Collections.emptyList());
        // We will not call getResult because we use getMetadata and getRecords
        Assert.assertEquals(strategy.getResultCalls(), 0);

        basic.reset();
        Assert.assertEquals(strategy.getResetCalls(), 1);

        basic.resetForPartition();
        Assert.assertEquals(strategy.getResetCalls(), 2);
    }

    @Test
    public void testEverythingClosedOnlyIfStrategyIsClosed() {
        ClosableStrategy strategy = new ClosableStrategy();
        Basic basic = new Basic(strategy, null, config);
        Assert.assertEquals(strategy.getClosedCalls(), 0);

        Assert.assertFalse(basic.isClosed());
        Assert.assertEquals(strategy.getClosedCalls(), 1);
        Assert.assertFalse(basic.isClosedForPartition());
        Assert.assertEquals(strategy.getClosedCalls(), 2);

        strategy.setClosed(true);

        Assert.assertTrue(basic.isClosed());
        Assert.assertEquals(strategy.getClosedCalls(), 3);
        Assert.assertTrue(basic.isClosedForPartition());
        Assert.assertEquals(strategy.getClosedCalls(), 4);
    }

    @Test
    public void testDisablingMetadataDoesNotGetStrategyMetadata() {
        // Metadata is disabled so Strategy metadata will not be collected.
        addMetadata(config, (List<Map.Entry<Meta.Concept, String>>) null);
        Basic basic = new Basic(strategy, null, config);

        Assert.assertEquals(strategy.getMetadataCalls(), 0);
        Meta meta = basic.getMetadata();
        Assert.assertNotNull(meta);
        Assert.assertTrue(meta.asMap().isEmpty());
        Assert.assertEquals(strategy.getMetadataCalls(), 0);
    }

    @Test
    public void testDisablingWindowMetadataDoesGetStrategyMetadata() {
        // Metadata is enabled but no keys for the window metadata. Strategy metadata will be collected.
        addMetadata(config, Collections.emptyList());
        Basic basic = new Basic(strategy, null, config);

        Assert.assertEquals(strategy.getMetadataCalls(), 0);
        Meta meta = basic.getMetadata();
        Assert.assertNotNull(meta);
        Assert.assertTrue(meta.asMap().isEmpty());
        Assert.assertEquals(strategy.getMetadataCalls(), 1);
    }

    @Test
    public void testResettingIncrementsWindowCount() {
        addMetadata(config, ALL_METADATA);
        Basic basic = new Basic(strategy, null, config);
        Assert.assertEquals(strategy.getResetCalls(), 0);

        Meta meta = basic.getMetadata();
        Assert.assertNotNull(meta);
        Map<String, Object> asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("num"), 1L);
        Assert.assertEquals(strategy.getMetadataCalls(), 1);

        basic.reset();
        Assert.assertEquals(strategy.getResetCalls(), 1);
        basic.reset();
        Assert.assertEquals(strategy.getResetCalls(), 2);
        basic.reset();
        Assert.assertEquals(strategy.getResetCalls(), 3);

        meta = basic.getMetadata();
        Assert.assertNotNull(meta);
        asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("num"), 4L);
        Assert.assertEquals(strategy.getMetadataCalls(), 2);
        Assert.assertEquals(strategy.getResetCalls(), 3);
    }
}
