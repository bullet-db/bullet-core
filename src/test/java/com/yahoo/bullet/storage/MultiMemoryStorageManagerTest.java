/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

public class MultiMemoryStorageManagerTest {
    private MultiMemoryStorageManager<Serializable> manager;

    @BeforeMethod
    private void setup() {
        StorageConfig config = new StorageConfig((String) null);
        config.set(StorageConfig.NAMESPACES, Arrays.asList("foo", "bar"));
        config.set(StorageConfig.PARTITION_COUNT, 10);
        manager = new MultiMemoryStorageManager<>(config);
    }

    @Test
    public void testNamespacedKeyStorage() throws Exception {
        Assert.assertTrue(manager.putString("foo", "a", "valueA").get());
        Assert.assertTrue(manager.putString("foo", "b", "valueB").get());
        Assert.assertTrue(manager.putString("bar", "a", "valueC").get());
        Assert.assertTrue(manager.putString("c", "valueD").get());

        Map<String, String> data;

        data = manager.getAllString("foo", new HashSet<>(Arrays.asList("a", "b"))).get();
        Assert.assertEquals(data.size(), 2);
        Assert.assertEquals(data.get("a"), "valueA");
        Assert.assertEquals(data.get("b"), "valueB");

        data = manager.getAllString("bar", new HashSet<>(Collections.singletonList("a"))).get();
        Assert.assertEquals(data.size(), 1);
        Assert.assertEquals(data.get("a"), "valueC");

        String defaultNamespace = manager.getDefaultNamespace();
        Assert.assertTrue(defaultNamespace.equals("foo") || defaultNamespace.equals("bar"));
        Assert.assertEquals(manager.getString(defaultNamespace, "c").get(), "valueD");
    }
}
