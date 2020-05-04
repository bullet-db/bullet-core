/*
 *  Copyright 2016, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.common;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RandomPoolTest {
    @Test
    public void testDefaultOverrides() {
        RandomPool<String> poolA = new RandomPool<>(null);
        RandomPool<String> poolB = new RandomPool<>(null);
        Assert.assertTrue(poolA.equals(poolA));
        Assert.assertEquals(poolA.hashCode(), poolA.hashCode());

        Assert.assertFalse(poolA.equals(null));
        Assert.assertFalse(poolA.equals("foo"));

        Assert.assertTrue(poolA.equals(poolB));
        Assert.assertEquals(poolA.hashCode(), poolB.hashCode());

        poolA = new RandomPool<>(Collections.singletonList("foo"));
        poolB = new RandomPool<>(Collections.singletonList("foo"));
        Assert.assertTrue(poolA.equals(poolB));
        Assert.assertEquals(poolA.hashCode(), poolB.hashCode());

        poolB = new RandomPool<>(Collections.singletonList("bar"));
        Assert.assertFalse(poolA.equals(poolB));

        List<String> contents = Collections.singletonList("foo");
        poolA = new RandomPool<>(contents);
        poolB = new RandomPool<>(contents);
        Assert.assertTrue(poolA.equals(poolB));
        Assert.assertEquals(poolA.hashCode(), poolB.hashCode());
    }

    @Test
    public void testToString() {
        RandomPool<String> pool = new RandomPool<>(null);
        Assert.assertNull(pool.toString());
        pool = new RandomPool<>(Collections.singletonList("foo"));
        Assert.assertEquals(pool.toString(), Collections.singletonList("foo").toString());
    }

    @Test
    public void testEmptyCase() {
        RandomPool<Integer> pool = new RandomPool<>(null);
        Assert.assertNull(pool.get());
        pool = new RandomPool<>(Collections.emptyList());
        Assert.assertNull(pool.get());
    }

    @Test
    public void testRandomGet() {
        List<Integer> list = Arrays.asList(1, 3, 4);
        Map<Integer, Integer> map = list.stream().collect(Collectors.toMap(Function.identity(), x -> 0));
        RandomPool<Integer> pool = new RandomPool<>(list);
        for (int i = 0; i < 1000; ++i) {
            int item = pool.get();
            map.put(item, map.get(item) + 1);
        }
        // That this is false is 1 - (2/3)^1000
        Assert.assertTrue(map.values().stream().allMatch(v -> v > 0));
    }

    @Test
    public void testGetReturnsNullAfterClear() {
        List<Integer> list = Arrays.asList(1, 3, 4);
        RandomPool<Integer> pool = new RandomPool<>(list);
        pool.clear();
        Assert.assertNull(pool.get());
    }
}
