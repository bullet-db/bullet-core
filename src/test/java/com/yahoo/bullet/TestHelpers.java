/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.yahoo.bullet.operations.SerializerDeserializer;
import com.yahoo.bullet.record.BulletRecord;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
}
