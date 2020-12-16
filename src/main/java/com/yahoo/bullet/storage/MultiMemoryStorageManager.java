/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import com.yahoo.bullet.common.BulletConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

@Slf4j
public class MultiMemoryStorageManager extends StorageManager<Serializable> implements Serializable {
    private static final long serialVersionUID = 9019357859078979031L;
    private static final CompletableFuture<Boolean> SUCCESS = CompletableFuture.completedFuture(true);

    private int partitions;
    private Set<String> namespaces;
    private String currentNamespace;
    private Map<String, Map<String, byte[]>> storage;
    private Map<String, byte[]> currentStorage;
    private Map<String, Map<Integer, Set<String>>> namespacePartitions;
    private Map<Integer, Set<String>> currentNamespacePartition;

    /**
     * Constructor.
     *
     * @param config The {@link BulletConfig} to create this manager with.
     */
    @SuppressWarnings("unchecked")
    public MultiMemoryStorageManager(BulletConfig config) {
        super(config);
        this.config = new StorageConfig(config);
        namespaces = (Set<String>) this.config.getAs(StorageConfig.MEMORY_NAMESPACES, Set.class);
        partitions = this.config.getAs(StorageConfig.MEMORY_PARTITION_COUNT, Integer.class);
        storage = new HashMap<>();
        namespacePartitions = new HashMap<>();
        // Use the first one by default
        use(namespaces.iterator().next());
        log.info("Using initial namespace {}", currentNamespace);
    }

    @Override
    public CompletableFuture<byte[]> get(String id) {
        return CompletableFuture.completedFuture(currentStorage.get(id));
    }

    @Override
    public CompletableFuture<Map<String, byte[]>> getPartition(int partition) {
        Set<String> mappings = currentNamespacePartition.get(partition);
        Map<String, byte[]> data = new HashMap<>();
        mappings.forEach(key -> data.put(key, currentStorage.get(key)));
        return CompletableFuture.completedFuture(data);
    }

    @Override
    public CompletableFuture<Map<String, byte[]>> getAll() {
        return CompletableFuture.completedFuture(new HashMap<>(currentStorage));
    }

    @Override
    public CompletableFuture<Map<String, byte[]>> getAll(Set<String> ids) {
        if (ids == null) {
            return CompletableFuture.completedFuture(null);
        }
        Map<String, byte[]> data = new HashMap<>();
        ids.forEach(id -> data.put(id, currentStorage.get(id)));
        return CompletableFuture.completedFuture(data);
    }

    @Override
    public CompletableFuture<Boolean> put(String id, byte[] value) {
        addToPartition(id, value);
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Boolean> putAll(Map<String, byte[]> data) {
        if (data != null) {
            data.forEach(this::addToPartition);
        }
        return SUCCESS;
    }

    @Override
    public CompletableFuture<byte[]> remove(String id) {
        return CompletableFuture.completedFuture(removeFromPartition(id));
    }

    @Override
    public CompletableFuture<Boolean> clear() {
        currentStorage.clear();
        currentNamespacePartition.clear();
        use(currentNamespace);
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Boolean> clear(Set<String> ids) {
        if (ids != null) {
            ids.forEach(this::removeFromPartition);
        }
        return SUCCESS;
    }

    @Override
    public int numberOfPartitions() {
        return partitions;
    }

    @Override
    public CompletableFuture<Boolean> clear(int partition) {
        Set<String> mappings = currentNamespacePartition.get(partition);
        mappings.forEach(currentStorage::remove);
        return SUCCESS;
    }

    @Override
    public void use(String namespace) {
        if (!namespaces.contains(namespace)) {
            log.error("{} not found in {}", namespace, namespaces);
            throw new RuntimeException("Unknown namespace " + namespace);
        }
        currentNamespacePartition = namespacePartitions.computeIfAbsent(namespace, k -> emptyPartitions());
        currentStorage = storage.computeIfAbsent(namespace, k -> new HashMap<>());
        currentNamespace = namespace;
    }

    @Override
    public String getNamespace() {
        return currentNamespace;
    }

    @Override
    public CompletableFuture<Boolean> clear(String namespace) {
        storage.remove(namespace);
        namespacePartitions.remove(namespace);
        // In case namespace was the currentNamespace
        use(currentNamespace);
        return SUCCESS;
    }

    private void addToPartition(String key, byte[] value) {
        int partition = partition(key);
        currentNamespacePartition.get(partition).add(key);
        currentStorage.put(key, value);
    }

    private byte[] removeFromPartition(String key) {
        int partition = partition(key);
        currentNamespacePartition.get(partition).remove(key);
        return currentStorage.remove(key);
    }

    private Map<Integer, Set<String>> emptyPartitions() {
        Map<Integer, Set<String>> partitions = new HashMap<>();
        IntStream.range(0, this.partitions).forEach(i -> partitions.put(i, new HashSet<>()));
        return partitions;
    }

    private int partition(String key) {
        return Math.floorMod(key == null ? 42 : key.hashCode(), partitions);
    }
}
