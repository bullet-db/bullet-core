/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common.metrics;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class MetricEventPublisherTest {
    private static class NullMetricEventPublisher implements MetricEventPublisher {
        private CompletableFuture<MetricEvent> payloadFuture = new CompletableFuture<>();
        private int published = 0;
        private int fired = 0;

        @Override
        public Map<String, String> getDimensions(Map<String, String> extraDimensions) {
            Map<String, String> map = MetricEventPublisher.super.getDimensions(extraDimensions);
            map.put("qux", "norf");
            return map;
        }

        @Override
        public CompletableFuture<Boolean> publish(MetricEvent payload) {
            published++;
            payloadFuture.complete(payload);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public void fire(Map<String, String> dimensions, Map<String, Number> metrics) {
            fired++;
            MetricEventPublisher.super.fire(dimensions, metrics);
        }

        @Override
        public void close() {
        }

        private MetricEvent getPayload() {
            try {
                return payloadFuture.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void testGroup() {
        MetricEventPublisher publisher = new NullMetricEventPublisher();
        MetricEvent event = publisher.convert(null, null);
        Assert.assertEquals(event.getGroup(), MetricEventPublisher.DEFAULT_GROUP);
        Assert.assertNull(event.getDimensions());
        Assert.assertNull(event.getMetrics());
    }

    @Test
    public void testPublishingEmptyMetrics() throws Exception {
        MetricEventPublisher publisher = new NullMetricEventPublisher();
        boolean result = publisher.publish(emptyMap()).get();
        Assert.assertTrue(result);
    }

    @Test
    public void testPublishingSingleMetric() throws Exception {
        NullMetricEventPublisher publisher = new NullMetricEventPublisher();
        long start = System.currentTimeMillis();
        publisher.publish("foo", 1.0).get();
        long end = System.currentTimeMillis();

        MetricEvent event = publisher.getPayload();
        Assert.assertEquals(event.getGroup(), MetricEventPublisher.DEFAULT_GROUP);
        Assert.assertEquals(event.getDimensions(), singletonMap("qux", "norf"));
        Assert.assertEquals(event.getMetrics(), singletonMap("foo", 1.0));
        Assert.assertTrue(event.getTimestamp() >= start && event.getTimestamp() <= end);
    }

    @Test
    public void testPublishingMetrics() {
        NullMetricEventPublisher publisher = new NullMetricEventPublisher();
        publisher.publish(singletonMap("baz", 1L));

        MetricEvent event = publisher.getPayload();
        Assert.assertEquals(event.getGroup(), MetricEventPublisher.DEFAULT_GROUP);
        Assert.assertEquals(event.getDimensions(), singletonMap("qux", "norf"));
        Assert.assertEquals(event.getMetrics(), singletonMap("baz", 1L));
    }

    @Test
    public void testPublishingMetricsAndDimensions() {
        NullMetricEventPublisher publisher = new NullMetricEventPublisher();
        publisher.publish(singletonMap("foo", "bar"), singletonMap("baz", 1L));

        MetricEvent event = publisher.getPayload();
        Map<String, String> expectedDimensions = new HashMap<>();
        expectedDimensions.put("foo", "bar");
        expectedDimensions.put("qux", "norf");
        Assert.assertEquals(event.getGroup(), MetricEventPublisher.DEFAULT_GROUP);
        Assert.assertEquals(event.getDimensions(), expectedDimensions);
        Assert.assertEquals(event.getMetrics(), singletonMap("baz", 1L));
    }

    @Test
    public void testFiringEmptyMetrics() {
        NullMetricEventPublisher publisher = new NullMetricEventPublisher();
        publisher.fire(emptyMap());
        Assert.assertEquals(publisher.fired, 1);
        Assert.assertEquals(publisher.published, 0);
    }

    @Test
    public void testFiringSingleMetric() {
        NullMetricEventPublisher publisher = new NullMetricEventPublisher();
        publisher.fire("foo", 1.0);

        MetricEvent event = publisher.getPayload();
        Assert.assertEquals(event.getGroup(), MetricEventPublisher.DEFAULT_GROUP);
        Assert.assertEquals(event.getDimensions(), singletonMap("qux", "norf"));
        Assert.assertEquals(event.getMetrics(), singletonMap("foo", 1.0));
    }

    @Test
    public void testFiringMetrics() {
        NullMetricEventPublisher publisher = new NullMetricEventPublisher();
        publisher.fire(singletonMap("baz", 1L));

        MetricEvent event = publisher.getPayload();
        Assert.assertEquals(event.getGroup(), MetricEventPublisher.DEFAULT_GROUP);
        Assert.assertEquals(event.getDimensions(), singletonMap("qux", "norf"));
        Assert.assertEquals(event.getMetrics(), singletonMap("baz", 1L));
    }

    @Test
    public void testFiringMetricsAndDimensions() {
        NullMetricEventPublisher publisher = new NullMetricEventPublisher();
        publisher.fire(singletonMap("foo", "bar"), singletonMap("baz", 1L));

        MetricEvent event = publisher.getPayload();
        Map<String, String> expectedDimensions = new HashMap<>();
        expectedDimensions.put("foo", "bar");
        expectedDimensions.put("qux", "norf");
        Assert.assertEquals(event.getGroup(), MetricEventPublisher.DEFAULT_GROUP);
        Assert.assertEquals(event.getDimensions(), expectedDimensions);
        Assert.assertEquals(event.getMetrics(), singletonMap("baz", 1L));
    }
}
