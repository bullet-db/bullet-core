/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import com.yahoo.bullet.common.BulletConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

/**
 * A Storage that stores everything in-memory and supports namespaces and partitions.
 */
@Slf4j
public class MultiMemoryStorageManager<V extends Serializable> extends StorageManager<V> implements Serializable {
    private static final long serialVersionUID = 9019357859078979031L;

    private int partitions;
    private Set<String> namespaces;
    private String defaultNamespace;

    private Map<String, Map<Integer, Map<String, byte[]>>> storage;

    /**
     * Constructor.
     *
     * @param config The {@link BulletConfig} to create this manager with.
     */
    @SuppressWarnings("unchecked")
    public MultiMemoryStorageManager(BulletConfig config) {
        super(config);
        this.config = new StorageConfig(config);
        partitions = this.config.getAs(StorageConfig.PARTITION_COUNT, Integer.class);
        namespaces = (Set<String>) this.config.getAs(StorageConfig.NAMESPACES, Set.class);
        defaultNamespace = namespaces.iterator().next();
        initializeStorage();
    }

    @Override
    protected CompletableFuture<Boolean> putRaw(String namespace, String id, byte[] value) {
        validateNamespace(namespace);
        return null;
    }

    @Override
    protected CompletableFuture<byte[]> getRaw(String namespace, String id) {
        validateNamespace(namespace);
        return null;
    }

    @Override
    protected CompletableFuture<Map<String, byte[]>> getAllRaw(String namespace) {
        validateNamespace(namespace);
        return null;
    }

    @Override
    protected CompletableFuture<byte[]> removeRaw(String namespace, String id) {
        validateNamespace(namespace);
        return null;
    }

    @Override
    public CompletableFuture<Boolean> clear() {
        initializeStorage();
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Boolean> clear(String namespace) {
        validateNamespace(namespace);
        storage.put(namespace, emptyPartitions());
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Boolean> clear(String namespace, Set<String> ids) {
        validateNamespace(namespace);
        if (ids == null) {
            return SUCCESS;
        }
        Map<Integer, Map<String, byte[]>> data = storage.get(namespace);
        for (String id: ids) {
            data.get(hash(id, partitions)).remove(id);
        }
        return SUCCESS;
    }

    @Override
    public int numberOfPartitions() {
        return partitions;
    }

    @Override
    public CompletableFuture<Map<String, V>> getPartition(String namespace, int partition) {
        validateNamespace(namespace);
        validatePartition(partition);
        Map<String, byte[]> data = storage.get(namespace).get(partition);
        return CompletableFuture.completedFuture(new HashMap<>(toObjectMap(data, this::convert)));
    }

    @Override
    public CompletableFuture<Boolean> clear(String namespace, int partition) {
        return super.clear(namespace, partition);
    }

    @Override
    public CompletableFuture<Boolean> repartition(int newPartitionCount) {
        if (newPartitionCount < 0) {
            throw new IllegalArgumentException("New partition count must be positive!");
        }
        partitions = newPartitionCount;
        Map<String, Map<Integer, Map<String, byte[]>>> newStorage = new HashMap<>();
        for (Map.Entry<String, Map<Integer, Map<String, byte[]>>> data : storage.entrySet()) {
            String namespace = data.getKey();
            newStorage.put(namespace, repartition(data.getValue().values()));
        }
        storage = newStorage;
        return SUCCESS;
    }

    @Override
    protected String getDefaultNamespace() {
        return defaultNamespace;
    }

    private Map<Integer, Map<String, byte[]>> repartition(Collection<Map<String, byte[]>> oldPartitions) {
        Map<Integer, Map<String, byte[]>> data = emptyPartitions();
        for (Map<String, byte[]> partition: oldPartitions) {
            for (Map.Entry<String, byte[]> entry: partition.entrySet()) {
                String key = entry.getKey();
                Integer hash = hash(key, partitions);
                data.get(hash).put(key, entry.getValue());
            }
        }
        return data;
    }

    private void validateNamespace(String namespace) {
        if (namespaces.contains(namespace)) {
            log.error("Namespace {} is not one of {}", namespace, namespaces);
            throw new IllegalArgumentException("The provided namespace is not a valid namespace: " + namespace);
        }
    }

    private void validatePartition(int partition) {
        if (partition >= partitions) {
            log.error("Partition {} is not between 0 and {} exclusive", partition, partitions);
            throw new IllegalArgumentException("The provided partition is not valid: " + partition);
        }
    }

    private void initializeStorage() {
        storage = new HashMap<>();
        namespaces.forEach(namespace -> storage.put(namespace, emptyPartitions()));
    }

    private Map<Integer, Map<String, byte[]>> emptyPartitions() {
        Map<Integer, Map<String, byte[]>> partitions = new HashMap<>();
        IntStream.range(0, this.partitions).forEach(i -> partitions.put(i, new HashMap<>()));
        return partitions;
    }
}
