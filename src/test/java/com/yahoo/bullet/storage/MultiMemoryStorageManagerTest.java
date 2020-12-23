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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class MultiMemoryStorageManagerTest {
    private MultiMemoryStorageManager<Serializable> manager;
    private int partitions = 10;

    private Map<Integer, Map<String, String>> getNonEmptyPartitions(String namespace) throws Exception {
        Map<Integer, Map<String, String>> allPartitions = new HashMap<>();
        for (int i = 0; i < partitions; ++i) {
            Map<String, String> data = manager.getPartitionString(namespace, i).get();
            if (!(data == null || data.isEmpty())) {
                allPartitions.put(i, data);
            }
        }
        return allPartitions;
    }

    @BeforeMethod
    private void setup() {
        StorageConfig config = new StorageConfig((String) null);
        config.set(StorageConfig.NAMESPACES, Arrays.asList("foo", "bar"));
        config.set(StorageConfig.PARTITION_COUNT, partitions);
        manager = new MultiMemoryStorageManager<>(config);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidNamespace() throws Exception {
        manager.put("dne", "key", new HashSet<>()).get();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidPartition() throws Exception {
        manager.getPartition(42).get();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidRepartition() throws Exception {
        manager.repartition(-1).get();
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

    @Test
    public void testSingleRetrieval() throws Exception {
        Assert.assertTrue(manager.putString("foo", "a", "valueA").get());
        Assert.assertEquals(manager.getString("foo", "a").get(), "valueA");
        Assert.assertEquals(manager.removeString("foo", "a").get(), "valueA");
        Assert.assertNull(manager.get("foo", "a").get());
    }

    @Test
    public void testMultipleRetrieval() throws Exception {
        Assert.assertTrue(manager.putString("foo", "a", "valueA").get());
        Assert.assertTrue(manager.putString("foo", "b", "valueB").get());

        Map<String, String> data;

        data = manager.getAllString("foo").get();
        Assert.assertEquals(data.size(), 2);
        Assert.assertEquals(data.get("a"), "valueA");
        Assert.assertEquals(data.get("b"), "valueB");

        Assert.assertTrue(manager.putString("bar", "a", "1").get());
        Assert.assertTrue(manager.putString("bar", "b", "2").get());
        Assert.assertTrue(manager.putString("bar", "c", "3").get());
        Assert.assertTrue(manager.putString("bar", "d", "4").get());
        Assert.assertTrue(manager.putString("bar", "e", "5").get());
        Assert.assertTrue(manager.putString("bar", "f", "6").get());

        Map<Integer, Map<String, String>> allPartitions = getNonEmptyPartitions("bar");
        Assert.assertEquals(allPartitions.values().stream().mapToInt(Map::size).sum(), 6);

        // For coverage
        manager.clear("bar", null);
        manager.clear("bar", new HashSet<>(Arrays.asList("a", "d", "f")));
        Assert.assertNull(manager.get("bar", "a").get());
        Assert.assertNull(manager.get("bar", "d").get());
        Assert.assertNull(manager.get("bar", "f").get());

        int partition = allPartitions.keySet().iterator().next();
        manager.clear("bar", partition);
        for (String key : allPartitions.get(partition).keySet()) {
            Assert.assertNull(manager.get("bar", key).get());
        }

        manager.clear("bar");
        Assert.assertEquals(manager.getAll("bar").get().size(), 0);
        Assert.assertEquals(manager.getAll("foo").get().size(), 2);

        manager.wipe();
        Assert.assertEquals(manager.getAll("foo").get().size(), 0);
        Assert.assertEquals(manager.getAll("bar").get().size(), 0);
    }

    @Test
    public void testPartitioning() throws Exception {
        Map<String, String> data = new HashMap<>();
        data.put("a", "1");
        data.put("b", "2");
        data.put("c", "3");
        data.put("d", "4");
        data.put("e", "5");
        data.put("f", "6");

        Assert.assertTrue(manager.putAllString("foo", data).get());
        Assert.assertTrue(manager.putAllString("bar", data).get());

        Map<Integer, Map<String, String>> allPartitionsFoo = getNonEmptyPartitions("foo");
        Assert.assertEquals(allPartitionsFoo.values().stream().mapToInt(Map::size).sum(), 6);

        Map<Integer, Map<String, String>> allPartitionsBar = getNonEmptyPartitions("bar");
        Assert.assertEquals(allPartitionsBar.values().stream().mapToInt(Map::size).sum(), 6);

        Assert.assertEquals(manager.numberOfPartitions(), partitions);
        Assert.assertEquals(manager.numberOfPartitions("foo"), partitions);
        Assert.assertEquals(manager.numberOfPartitions("bar"), partitions);

        manager.repartition("foo", 1);
        Assert.assertEquals(manager.numberOfPartitions("foo"), 1);

        Map<String, String> expected = new HashMap<>();
        allPartitionsFoo.values().forEach(expected::putAll);

        Assert.assertEquals(manager.getAllString("foo").get(), expected);
    }
}
