/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

public class NullStorageManagerTest {
    private NullStorageManager<Serializable> manager;

    @BeforeClass
    public void setup() {
        manager = new NullStorageManager<>(null);
    }

    @AfterClass
    public void teardown() {
        manager.close();
    }

    @Test
    public void testGet() throws Exception {
        Assert.assertNull(manager.get("foo").get());
        Assert.assertNull(manager.get("foo", "bar").get());
        Assert.assertNull(manager.getString("foo").get());
        Assert.assertNull(manager.getString("foo", "bar").get());
    }

    @Test
    public void testRemove() throws Exception {
        Assert.assertNull(manager.remove("foo").get());
        Assert.assertNull(manager.remove("foo", "bar").get());
        Assert.assertNull(manager.removeString("foo").get());
        Assert.assertNull(manager.removeString("foo", "bar").get());
    }

    @Test
    public void testRemoveMultiple() throws Exception {
        Assert.assertTrue(manager.clear("foo").get());
        Assert.assertTrue(manager.clear("foo", null).get());
        Assert.assertTrue(manager.clear("foo", Collections.emptySet()).get());
        Assert.assertTrue(manager.clear("foo", Collections.singleton("bar")).get());
        Assert.assertTrue(manager.clear("foo", 0).get());
        Assert.assertTrue(manager.clear().get());
        Assert.assertTrue(manager.wipe().get());
    }

    @Test
    public void testPut() throws Exception {
        Assert.assertTrue(manager.put("foo", "bar".getBytes()).get());
        Assert.assertTrue(manager.put("foo", null).get());
        Assert.assertTrue(manager.put(null, null).get());
        Assert.assertTrue(manager.put("foo", "bar", "baz".getBytes()).get());
        Assert.assertTrue(manager.put("foo", "bar", null).get());
        Assert.assertTrue(manager.put("foo", null, null).get());
        Assert.assertTrue(manager.put(null, null, null).get());
        Assert.assertTrue(manager.putString("foo", "bar").get());
        Assert.assertTrue(manager.putString("foo", null).get());
        Assert.assertTrue(manager.putString(null, null).get());
        Assert.assertTrue(manager.putString("foo", "bar", "baz").get());
        Assert.assertTrue(manager.putString("foo", "bar", null).get());
        Assert.assertTrue(manager.putString("foo", null, null).get());
        Assert.assertTrue(manager.putString(null, null, null).get());
    }

    @Test
    public void testPutAll() throws Exception {
        Assert.assertTrue(manager.putAll(null).get());
        Assert.assertTrue(manager.putAll(Collections.emptyMap()).get());
        Assert.assertTrue(manager.putAll(null, null).get());
        Assert.assertTrue(manager.putAll(null, Collections.emptyMap()).get());
        Assert.assertTrue(manager.putAll("", Collections.emptyMap()).get());
        Assert.assertTrue(manager.putAll("", Collections.singletonMap("foo", "bar")).get());
        Assert.assertTrue(manager.putAllString(null).get());
        Assert.assertTrue(manager.putAllString(null, null).get());
        Assert.assertTrue(manager.putAllString(null, Collections.emptyMap()).get());
        Assert.assertTrue(manager.putAllString("", null).get());
    }

    @Test
    public void testGetAll() throws Exception {
        Assert.assertNull(manager.getAll().get());
        Assert.assertNull(manager.getAllString().get());
        Assert.assertNull(manager.getAllString((Set<String>) null).get());
        Assert.assertTrue(manager.getAllString(Collections.emptySet()).get().isEmpty());
        Assert.assertTrue(manager.getAllString(Collections.singleton("foo")).get().isEmpty());
        Assert.assertNull(manager.getAllString(null, null).get());
        Assert.assertNull(manager.getAllString("", null).get());
        Assert.assertTrue(manager.getAllString(null, Collections.singleton("foo")).get().isEmpty());
        Assert.assertTrue(manager.getAllString("", Collections.singleton("foo")).get().isEmpty());
        Assert.assertNull(manager.getPartition(1).get());
        Assert.assertNull(manager.getPartition(null, 1).get());
        Assert.assertNull(manager.getPartition("", 1).get());
    }

    @Test
    public void testRepartitioning() throws Exception {
        Assert.assertEquals(manager.numberOfPartitions(), 0);
        Assert.assertEquals(manager.numberOfPartitions(""), 0);
        Assert.assertEquals(manager.numberOfPartitions(null), 0);
        Assert.assertTrue(manager.repartition(10).get());
        Assert.assertTrue(manager.repartition("", 10).get());
        Assert.assertEquals(manager.numberOfPartitions(), 0);
    }
}
