/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MemoryStorageManagerTest {
    private MemoryStorageManager<Serializable> manager;

    @BeforeMethod
    public void setup() {
        manager = new MemoryStorageManager<>(null);
    }

    @AfterMethod
    public void teardown() throws Exception {
        manager.clear().get();
        manager.close();
    }

    @Test
    public void testSingleKeys() throws Exception {
        Assert.assertNull(manager.get("foo").get());
        Assert.assertNull(manager.get("bar", "foo").get());
        Assert.assertTrue(manager.put("bar", "foo", "test").get());
        Assert.assertEquals(manager.get("bar", "foo").get(), "test");
        Assert.assertEquals(manager.remove("bar", "foo").get(), "test");
        Assert.assertNull(manager.get("foo").get());
        Assert.assertNull(manager.get("bar", "foo").get());
    }

    @Test
    public void testNoNamespace() throws Exception {
        Assert.assertTrue(manager.put("bar", "foo", "test").get());
        Assert.assertEquals(manager.get("foo").get(), "test");
        Assert.assertTrue(manager.put("foo", "bar").get());
        Assert.assertTrue(manager.clear("baz").get());
    }

    @Test
    public void testClearing() throws Exception {
        Assert.assertTrue(manager.put("a", "foo", "test").get());
        Assert.assertTrue(manager.put("b", "bar", "baz").get());
        Assert.assertTrue(manager.clear(Collections.singleton("foo")).get());
        Assert.assertNull(manager.get("foo").get());
        Assert.assertEquals(manager.get("bar").get(), "baz");
    }

    @Test
    public void testNoPartitions() throws Exception {
        Assert.assertTrue(manager.put("foo", "test").get());
        Assert.assertEquals(manager.numberOfPartitions(), 0);
        Assert.assertEquals(manager.get("foo").get(), "test");
        Assert.assertTrue(manager.clear(42).get());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testStoringObjects() throws Exception {
        Serializable set = new HashSet<>(Arrays.asList("foo", "bar"));
        Serializable list = new ArrayList<>(Arrays.asList("foo", "bar", "baz"));
        Assert.assertTrue(manager.put("set", set).get());
        Assert.assertTrue(manager.put("list", list).get());
        Set<String> asSet = (Set<String>) manager.get("set").get();
        List<String> asList = (List<String>) manager.get("list").get();
        Assert.assertEquals(asSet, set);
        Assert.assertEquals(asList, list);
    }

    @Test
    public void testMultipleKeys() throws Exception {
        Map<String, String> data = new HashMap<>();
        data.put("foo", "1");
        data.put("bar", "2");
        data.put("baz", "3");
        data.put("qux", "4");

        Assert.assertTrue(manager.putAllString(data).get());
        Map<String, String> actual = manager.getAllString(new HashSet<>(Arrays.asList("foo", "baz"))).get();
        Assert.assertEquals(actual.size(), 2);
        Assert.assertEquals(actual.get("foo"), "1");
        Assert.assertEquals(actual.get("baz"), "3");
        Assert.assertEquals(manager.getAll().get().size(), 4);
        Assert.assertEquals(manager.getAll(new HashSet<>(Arrays.asList("foo", "bar", "baz", "qux"))).get().size(), 4);
    }
}
