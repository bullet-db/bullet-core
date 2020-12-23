/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import com.yahoo.bullet.common.Config;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class StorageConfigTest {
    @Test
    public void testDefaults() {
        StorageConfig config = new StorageConfig((Config) null);
        Assert.assertEquals(config.get(StorageConfig.NAMESPACES),
                            Collections.singleton(StorageConfig.DEFAULT_NAMESPACE));
        Assert.assertNull(config.getAs(StorageConfig.PARTITION_COUNT, Integer.class));
    }

    @Test
    public void testCreation() {
        StorageConfig config = new StorageConfig("src/test/resources/storage_config.yaml");
        Assert.assertEquals(config.get(StorageConfig.NAMESPACES),
                            new HashSet<>(Arrays.asList("one", "two", "three", "eight")));
        Assert.assertEquals(config.getAs(StorageConfig.PARTITION_COUNT, Integer.class), (Integer) 15);
        Assert.assertEquals(config.getAs(StorageConfig.PREFIX + "fake.setting", String.class), "foo");
    }
}
