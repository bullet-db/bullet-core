/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import com.yahoo.bullet.common.BulletConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class StorageManagerTest {
    public static class MockStorageManager<V extends Serializable> extends StorageManager<V> implements Serializable {
        private static final long serialVersionUID = 7323669039166232486L;

        public MockStorageManager(BulletConfig config) {
            super(config);
        }

        @Override
        protected CompletableFuture<Boolean> putRaw(String namespace, String id, byte[] value) {
            return null;
        }

        @Override
        protected CompletableFuture<byte[]> getRaw(String namespace, String id) {
            return null;
        }

        @Override
        protected CompletableFuture<Map<String, byte[]>> getAllRaw(String namespace) {
            return null;
        }

        @Override
        protected CompletableFuture<byte[]> removeRaw(String namespace, String id) {
            return null;
        }

        @Override
        public CompletableFuture<Boolean> wipe() {
            return null;
        }

        @Override
        public CompletableFuture<Boolean> clear(String namespace) {
            return null;
        }

        @Override
        public CompletableFuture<Boolean> clear(String namespace, Set<String> ids) {
            return null;
        }
    }

    @Test
    public void testFrom() {
        StorageConfig config = new StorageConfig((String) null);
        config.set(BulletConfig.STORAGE_CLASS_NAME, MockStorageManager.class.getName());
        StorageManager manager = StorageManager.from(config);
        Assert.assertTrue(manager instanceof MockStorageManager);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "Cannot create.*")
    public void testFromWithABadClass() {
        StorageConfig config = new StorageConfig((String) null);
        config.set(BulletConfig.STORAGE_CLASS_NAME, "does.not.exist");
        StorageManager.from(config);
    }
}
