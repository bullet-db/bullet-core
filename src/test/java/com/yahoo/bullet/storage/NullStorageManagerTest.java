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

import java.util.ArrayList;
import java.util.Arrays;

public class NullStorageManagerTest {
    private NullStorageManager manager;

    @BeforeClass
    public void setup() {
        manager = new NullStorageManager(null);
    }

    @AfterClass
    public void teardown() {
        manager.close();
    }

    @Test
    public void testGet() throws Exception {
        Assert.assertNull(manager.get("foo").get());
        Assert.assertNull(manager.getString("foo").get());
        Assert.assertNull(manager.getObject("foo").get());
    }

    @Test
    public void testRemove() throws Exception {
        Assert.assertNull(manager.remove("foo").get());
        Assert.assertNull(manager.removeString("foo").get());
        Assert.assertNull(manager.removeObject("foo").get());
    }

    @Test
    public void testRemoveAll() throws Exception {
        Assert.assertTrue(manager.clear(null).get());
    }

    @Test
    public void testPut() throws Exception {
        Assert.assertTrue(manager.put("foo", "bar".getBytes()).get());
        Assert.assertTrue(manager.put("foo", null).get());
        Assert.assertTrue(manager.put(null, null).get());
        Assert.assertTrue(manager.putString("foo", "bar").get());
        Assert.assertTrue(manager.putString("foo", null).get());
        Assert.assertTrue(manager.putString(null, null).get());
        Assert.assertTrue(manager.putObject("foo", new ArrayList<>(Arrays.asList("foo", "bar"))).get());
    }

    @Test
    public void testPutAll() throws Exception {
        Assert.assertTrue(manager.putAll(null).get());
    }

    @Test
    public void testGetAll() throws Exception {
        Assert.assertNull(manager.getAll().get());
        Assert.assertNull(manager.getAllString().get());
    }

    @Test
    public void testClear() throws Exception {
        Assert.assertTrue(manager.clear().get());
    }
}
