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
import java.util.Map;

public class MultiMemoryCountingCriteriaTest {
    private MultiMemoryStorageManager<Serializable> manager;

    @BeforeMethod
    private void setup() {
        StorageConfig config = new StorageConfig((String) null);
        config.set(StorageConfig.NAMESPACES, Arrays.asList("foo", "bar"));
        config.set(StorageConfig.PARTITION_COUNT, 10);
        manager = new MultiMemoryStorageManager<>(config);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testNoGetAll() throws Exception {
        manager.getAll(new MultiMemoryCountingCriteria()).get();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidNamespace() throws Exception {
        MultiMemoryCountingCriteria criteria = new MultiMemoryCountingCriteria();
        manager.apply(criteria, Arrays.asList("dne", "foo")).get();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testInvalidStorage() throws Exception {
        MultiMemoryCountingCriteria criteria = new MultiMemoryCountingCriteria();
        new NullStorageManager<String>(null).apply(criteria, null).get();
    }

    @Test
    public void testRetrievingOnDefaultNamespace() throws Exception {
        Map<String, String> data = new HashMap<>();
        data.put("a", "1");
        data.put("b", "2");
        Assert.assertTrue(manager.putAllString(data).get());

        long defaultNamespaceCount = manager.getAll().get().size();
        MultiMemoryCountingCriteria criteria = new MultiMemoryCountingCriteria();
        Assert.assertEquals(manager.retrieveAll(criteria).get(), (Long) defaultNamespaceCount);
    }

    @Test
    public void testApplication() throws Exception {
        Map<String, String> data = new HashMap<>();
        data.put("a", "1");
        data.put("b", "2");

        Assert.assertTrue(manager.putAllString("foo", data).get());
        data.put("c", "3");
        Assert.assertTrue(manager.putAllString("bar", data).get());

        MultiMemoryCountingCriteria criteria = new MultiMemoryCountingCriteria();
        Assert.assertEquals(manager.apply(criteria, Collections.singletonList("foo")).get(), (Long) 2L);
        Assert.assertEquals(manager.apply(criteria, Collections.singletonList("bar")).get(), (Long) 3L);
        Assert.assertEquals(manager.apply(criteria, null).get(), (Long) 5L);
        Assert.assertEquals(manager.apply(criteria, Collections.emptyList()).get(), (Long) 5L);
    }
}
