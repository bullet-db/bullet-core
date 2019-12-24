/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common.metrics;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;

public class MetricCollectorTest {
    @Test
    public void testMetricsCollectorCreation() {
        MetricCollector collector = new MetricCollector(Arrays.asList("foo", "bar", "baz"));
        Map<String, Number> metrics = collector.extractMetrics();
        Assert.assertEquals(metrics.size(), 3);
        Assert.assertEquals(metrics.get("foo"), 0L);
        Assert.assertEquals(metrics.get("bar"), 0L);
        Assert.assertEquals(metrics.get("baz"), 0L);

        collector = new MetricCollector(Arrays.asList("foo", "bar", "baz"), Arrays.asList("qux", "norf"));
        metrics = collector.extractMetrics();
        Assert.assertEquals(metrics.size(), 5);
        Assert.assertEquals(metrics.get("foo"), 0L);
        Assert.assertEquals(metrics.get("bar"), 0L);
        Assert.assertEquals(metrics.get("baz"), 0L);
        Assert.assertEquals(metrics.get("qux"), 0.0);
        Assert.assertEquals(metrics.get("norf"), 0.0);
    }

    @Test
    public void testCounting() {
        MetricCollector collector = new MetricCollector();
        collector.add("foo", 4);
        collector.increment("foo");
        collector.increment("bar");

        Map<String, Number> metrics = collector.extractMetrics();
        Assert.assertEquals(metrics.size(), 2);
        Assert.assertEquals(metrics.get("foo"), 5L);
        Assert.assertEquals(metrics.get("bar"), 1L);
        Assert.assertNull(metrics.get("baz"));

        metrics = collector.extractMetrics();
        Assert.assertEquals(metrics.size(), 2);
        Assert.assertEquals(metrics.get("foo"), 0L);
        Assert.assertEquals(metrics.get("bar"), 0L);
        Assert.assertNull(metrics.get("baz"));
    }

    @Test
    public void testCountingAndAveraging() {
        MetricCollector collector = new MetricCollector(Arrays.asList("foo", "bar"), Arrays.asList("baz", "qux", "norf"));
        collector.add("foo", 4);
        collector.average("baz", 10);
        collector.average("qux", 4242, 100);

        Map<String, Number> metrics = collector.extractMetrics();
        Assert.assertEquals(metrics.size(), 5);
        Assert.assertEquals(metrics.get("foo"), 4L);
        Assert.assertEquals(metrics.get("bar"), 0L);
        Assert.assertEquals(metrics.get("baz"), 10.0);
        Assert.assertEquals(metrics.get("qux"), 42.42);
        Assert.assertEquals(metrics.get("norf"), 0.0);

        metrics = collector.extractMetrics();
        Assert.assertEquals(metrics.size(), 5);
        Assert.assertEquals(metrics.get("foo"), 0L);
        Assert.assertEquals(metrics.get("bar"), 0L);
        Assert.assertEquals(metrics.get("baz"), 0.0);
        Assert.assertEquals(metrics.get("qux"), 0.0);
        Assert.assertEquals(metrics.get("norf"), 0.0);
    }
}
