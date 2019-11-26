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

import java.util.Collections;

public class MemoryStorageManagerTest {
    private MemoryStorageManager manager;

    @BeforeMethod
    public void setup() {
        manager = new MemoryStorageManager(null);
    }

    @AfterMethod
    public void teardown() throws Exception {
        manager.clear().get();
        manager.close();
    }

    @Test
    public void testGet() throws Exception {
        Assert.assertNull(manager.get("foo").get());
        Assert.assertNull(manager.getString("foo").get());
    }

    @Test
    public void testRemove() throws Exception {
        Assert.assertNull(manager.remove("foo").get());
        Assert.assertNull(manager.removeString("foo").get());
    }

    @Test
    public void testRemoveAll() throws Exception {
        Assert.assertTrue(manager.clear(null).get());
        Assert.assertTrue(manager.putAll(Collections.singletonMap("foo", new byte[0])).get());
        Assert.assertEquals(manager.get("foo").get(), new byte[0]);
        Assert.assertTrue(manager.clear(Collections.singleton("bar")).get());
        Assert.assertEquals(manager.get("foo").get(), new byte[0]);
        Assert.assertTrue(manager.clear(Collections.singleton("foo")).get());
        Assert.assertNull(manager.get("foo").get());
    }

    @Test
    public void testPut() throws Exception {
        Assert.assertTrue(manager.put("foo", "bar".getBytes()).get());
        Assert.assertTrue(manager.put("foo", null).get());
        Assert.assertTrue(manager.put(null, null).get());
        Assert.assertTrue(manager.putString("foo", "bar").get());
        Assert.assertTrue(manager.putString("foo", null).get());
        Assert.assertTrue(manager.putString(null, null).get());
    }

    @Test
    public void testPutAll() throws Exception {
        Assert.assertTrue(manager.putAll(null).get());
        Assert.assertTrue(manager.putAll(Collections.singletonMap("foo", new byte[0])).get());
        Assert.assertEquals(manager.get("foo").get(), new byte[0]);
    }

    @Test
    public void testClear() throws Exception {
        Assert.assertTrue(manager.putAll(Collections.singletonMap("foo", new byte[0])).get());
        Assert.assertEquals(manager.get("foo").get(), new byte[0]);
    }

    @Test
    public void testGetAll() throws Exception {
        Assert.assertNull(manager.getAll().get());
        Assert.assertNull(manager.getAllString().get());
    }
}
