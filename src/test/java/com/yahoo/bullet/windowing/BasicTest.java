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
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.yahoo.bullet.TestHelpers.addMetadata;
import static java.util.Arrays.asList;

public class BasicTest {
    private class ClosableStrategy extends MockStrategy {
        @Setter
        private boolean closed = false;

        @Override
        public boolean isClosed() {
            super.isClosed();
            return closed;
        }
    }

    private static List<Map.Entry<Meta.Concept, String>> ALL_METADATA =
        asList(Pair.of(Meta.Concept.WINDOW_METADATA, "window_stats"),
               Pair.of(Meta.Concept.WINDOW_NAME, "name"),
               Pair.of(Meta.Concept.WINDOW_NUMBER, "num"));

    @Test
    public void testCreation() {
        BulletConfig config = new BulletConfig().validate();
        MockStrategy strategy = new MockStrategy();
        Basic basic = new Basic(strategy, null, config);
        Assert.assertNotNull(basic.getMetadata());
        Assert.assertFalse(basic.initialize().isPresent());
    }

    @Test
    public void testAllDataMethodsProxyToStrategy() {
        BulletConfig config = new BulletConfig();
        addMetadata(config, ALL_METADATA);

        MockStrategy strategy = new MockStrategy();
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

        Meta meta = basic.getMetadata();
        Assert.assertNotNull(meta);
        Map<String, Object> asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("name"), Basic.NAME);
        Assert.assertEquals(asMap.get("num"), 1L);
        Assert.assertEquals(strategy.getMetadataCalls(), 1);

        Assert.assertNull(basic.getRecords());
        Assert.assertEquals(strategy.getRecordCalls(), 1);

        Clip clip = basic.getResult();
        Assert.assertEquals(clip.getMeta().asMap(), meta.asMap());
        Assert.assertEquals(clip.getRecords(), Collections.emptyList());
        // We will not call getResult because we use getMetadata and getRecords
        Assert.assertEquals(strategy.getResultCalls(), 0);

        basic.reset();
        Assert.assertEquals(strategy.getResetCalls(), 1);
    }

    @Test
    public void testEverythingClosedOnlyIfStrategyIsClosed() {
        BulletConfig config = new BulletConfig().validate();
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
        BulletConfig config = new BulletConfig();
        addMetadata(config, (List<Map.Entry<Meta.Concept, String>>) null);
        MockStrategy strategy = new MockStrategy();
        Basic basic = new Basic(strategy, null, config);

        Assert.assertEquals(strategy.getMetadataCalls(), 0);
        Meta meta = basic.getMetadata();
        Assert.assertNotNull(meta);
        Assert.assertTrue(meta.asMap().isEmpty());
        Assert.assertEquals(strategy.getMetadataCalls(), 0);
    }

    @Test
    public void testResettingIncrementsWindowCount() {
        BulletConfig config = new BulletConfig();
        addMetadata(config, ALL_METADATA);
        MockStrategy strategy = new MockStrategy();
        Basic basic = new Basic(strategy, null, config);

        Meta meta = basic.getMetadata();
        Assert.assertNotNull(meta);
        Map<String, Object> asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("num"), 1L);
        Assert.assertEquals(strategy.getMetadataCalls(), 1);

        basic.reset();
        basic.reset();
        basic.reset();

        meta = basic.getMetadata();
        Assert.assertNotNull(meta);
        asMap = (Map<String, Object>) meta.asMap().get("window_stats");
        Assert.assertEquals(asMap.get("num"), 4L);
        Assert.assertEquals(strategy.getMetadataCalls(), 2);
        Assert.assertEquals(strategy.getResetCalls(), 3);
    }
}
