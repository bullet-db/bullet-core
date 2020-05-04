/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.common.metrics.MetricCollector;
import com.yahoo.bullet.common.metrics.MetricEventPublisher;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Meta;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.testng.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.mockito.AdditionalAnswers.returnsElementsOf;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class TestHelpers {
    public static final double EPSILON = 1E-6;

    public static <E> void assertContains(Collection<E> collection, E item) {
        Objects.requireNonNull(collection);
        assertContains(collection, item, 1);
    }

    public static <E> void assertContains(Collection<E> collection, E item, long times) {
        Objects.requireNonNull(collection);
        long actual = collection.stream().map(item::equals).filter(x -> x).count();
        Assert.assertEquals(actual, times);
    }

    @SuppressWarnings("unchecked")
    public static MetricEventPublisher mockMetricPublisher(boolean result) {
        MetricEventPublisher publisher = mock(MetricEventPublisher.class);
        doReturn(CompletableFuture.completedFuture(result))
                .when(publisher).publish((Map<String, String>) any(Map.class), (Map<String, Number>) any(Map.class));
        doReturn(CompletableFuture.completedFuture(result))
                .when(publisher).publish((Map<String, Number>) any(Map.class));
        doReturn(CompletableFuture.completedFuture(result))
                .when(publisher).publish(anyString(), any(Number.class));
        return publisher;
    }

    @SuppressWarnings("unchecked")
    public static MetricEventPublisher mockFailingMetricPublisher() {
        MetricEventPublisher publisher = mock(MetricEventPublisher.class);
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Testing"));
        doReturn(result).when(publisher).publish((Map<String, String>) any(Map.class), (Map<String, Number>) any(Map.class));
        doReturn(result).when(publisher).publish((Map<String, Number>) any(Map.class));
        doReturn(result).when(publisher).publish(anyString(), any(Number.class));
        return publisher;
    }
    
    public static CloseableHttpClient mockHTTPClient(int... statuses) {
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
        List<StatusLine> statusLines = new ArrayList<>();
        for (int status : statuses) {
            StatusLine mockStatus = mock(StatusLine.class);
            doReturn(status).when(mockStatus).getStatusCode();
            statusLines.add(mockStatus);
        }
        doAnswer(returnsElementsOf(statusLines)).when(mockResponse).getStatusLine();
        try {
            doReturn(mockResponse).when(client).execute(any());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return client;
    }

    public static void assertOnlyMetricEquals(MetricCollector collector, String key, Number value) {
        Map<String, Number> metrics = collector.extractMetrics();
        assertOnlyMetricEquals(metrics, metrics.size(), key, value);
    }
    
    public static void assertOnlyMetricEquals(MetricCollector collector, int size, String key, Number value) {
        assertOnlyMetricEquals(collector.extractMetrics(), size, key, value);
    }
    
    public static void assertOnlyMetricEquals(Map<String, Number> metrics, int size, String key, Number value) {
        Assert.assertEquals(metrics.size(), size, "Expected to have " + size + " metrics");
        assertMetricEquals(metrics, key, value);
        assertNoMetric(metrics, key);
    }

    public static void assertNoMetric(Map<String, Number> counts, String... exclusions) {
        Set<String> notThese = new HashSet<>(Arrays.asList(exclusions));
        for (Map.Entry<String, Number> metric : counts.entrySet()) {
            if (notThese.contains(metric.getKey())) {
                continue;
            }
            Assert.assertEquals(metric.getValue(), 0L, "Expected metric to be 0 for " + metric.getKey());
        }
    }

    public static void assertMetricEquals(Map<String, Number> counts, String key, Number value) {
        Assert.assertEquals(counts.get(key), value, "Metric for " + key + " was not " + value);
    }

    public static void assertJSONEquals(String actual, String expected) {
        assertJSONEquals(actual, expected, (String[]) null);
    }

    public static void assertJSONEquals(String actual, String expected, String... fieldsToIgnore) {
        JsonParser parser = new JsonParser();
        JsonElement first = parser.parse(actual);
        JsonElement second = parser.parse(expected);
        if (fieldsToIgnore != null) {
            for (String ignore : fieldsToIgnore) {
                ((JsonObject) first).remove(ignore);
                ((JsonObject) second).remove(ignore);
            }
        }
        Assert.assertEquals(first, second, "Actual: " + first + " Expected: " + second);
    }

    public static void assertApproxEquals(double actual, double expected) {
        assertApproxEquals(actual, expected, EPSILON);
    }

    public static void assertApproxEquals(double actual, double expected, double epsilon) {
        Assert.assertTrue(Math.abs(actual - expected) <= epsilon,
                          "Actual: " + actual + " Expected: " + expected + " Epsilon: " + epsilon);
    }

    public static byte[] getListBytes(BulletRecord... records) {
        ArrayList<BulletRecord> asList = new ArrayList<>();
        Collections.addAll(asList, records);
        return SerializerDeserializer.toBytes(asList);
    }

    public static BulletConfig addMetadata(BulletConfig config, List<Map.Entry<Meta.Concept, String>> metadata) {
        if (metadata == null) {
            config.set(BulletConfig.RESULT_METADATA_ENABLE, false);
            return config.validate();
        }
        List<Map<String, String>> metadataList = new ArrayList<>();
        for (Map.Entry<Meta.Concept, String> meta : metadata) {
            Map<String, String> entry = new HashMap<>();
            entry.put(BulletConfig.RESULT_METADATA_METRICS_CONCEPT_KEY, meta.getKey().getName());
            entry.put(BulletConfig.RESULT_METADATA_METRICS_NAME_KEY, meta.getValue());
            metadataList.add(entry);
        }
        config.set(BulletConfig.RESULT_METADATA_METRICS, metadataList);
        config.set(BulletConfig.RESULT_METADATA_ENABLE, true);
        return config.validate();
    }

    @SafeVarargs
    public static BulletConfig addMetadata(BulletConfig config, Map.Entry<Meta.Concept, String>... metadata) {
        return addMetadata(config, metadata == null ? null : Arrays.asList(metadata));
    }
}
