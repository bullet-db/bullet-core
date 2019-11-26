/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import com.yahoo.bullet.common.BulletConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class StorageManagerTest {
    public static class MockStorageManager extends StorageManager {
        private static final long serialVersionUID = 7323669039166232486L;

        public MockStorageManager(BulletConfig config) {
            super(config);
        }

        @Override
        public CompletableFuture<byte[]> get(String id) {
            return null;
        }

        @Override
        public CompletableFuture<byte[]> remove(String id) {
            return null;
        }

        @Override
        public CompletableFuture<Boolean> clear(Set<String> ids) {
            return null;
        }

        @Override
        public CompletableFuture<Boolean> put(String id, byte[] value) {
            return null;
        }

        @Override
        public CompletableFuture<Boolean> putAll(Map<String, byte[]> data) {
            return null;
        }

        @Override
        public CompletableFuture<Map<String, byte[]>> getAll() {
            return null;
        }

        @Override
        public CompletableFuture<Boolean> clear() {
            return null;
        }
    }

    @Test
    public void testFrom() {
        BulletConfig config = new BulletConfig();
        config.set(BulletConfig.STORAGE_CLASS_NAME, MockStorageManager.class.getName());
        Assert.assertTrue(StorageManager.from(config) instanceof MockStorageManager);
    }
}
