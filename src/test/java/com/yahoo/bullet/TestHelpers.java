/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.SerializerDeserializer;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Meta;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    public static void assertJSONEquals(String actual, String expected) {
        JsonParser parser = new JsonParser();
        JsonElement first = parser.parse(actual);
        JsonElement second = parser.parse(expected);
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
