/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import com.yahoo.bullet.common.BulletConfig;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

/**
 * A storage that stores everything in-memory and supports namespaces and partitions. It starts off with a fixed initial
 * number of partitions for all namespaces. You may use {@link #repartition(String, int)} to change it at runtime per
 * namespace.
 *
 * Supported criteria:
 * <ol>
 *     <li>{@link MultiMemoryCountingCriteria} that counts keys across namespaces</li>
 * </ol>
 */
@Slf4j
public class MultiMemoryStorageManager<V extends Serializable> extends StorageManager<V> implements Serializable {
    private static final long serialVersionUID = 9019357859078979031L;

    private Set<String> namespaces;
    private String defaultNamespace;
    private Map<String, Integer> partitions;

    @Getter(AccessLevel.PACKAGE)
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
        namespaces = (Set<String>) this.config.getAs(StorageConfig.NAMESPACES, Set.class);
        int defaultPartitions = this.config.getAs(StorageConfig.PARTITION_COUNT, Integer.class);
        partitions = new HashMap<>();
        namespaces.forEach(namespace -> partitions.put(namespace, defaultPartitions));
        // Pick the first one as the default
        defaultNamespace = namespaces.iterator().next();
        initializeStorage();
        log.info("Initialized storage with {} namepaces and {} partitions each", namespaces.size(), defaultPartitions);
    }

    @Override
    protected CompletableFuture<Boolean> putRaw(String namespace, String id, byte[] value) {
        validateNamespace(namespace);
        storage.get(namespace).get(hash(namespace, id)).put(id, value);
        return SUCCESS;
    }

    @Override
    protected CompletableFuture<byte[]> getRaw(String namespace, String id) {
        validateNamespace(namespace);
        return CompletableFuture.completedFuture(storage.get(namespace).get(hash(namespace, id)).get(id));
    }

    @Override
    protected CompletableFuture<Map<String, byte[]>> getAllRaw(String namespace) {
        validateNamespace(namespace);
        Map<String, byte[]> result = new HashMap<>();
        storage.get(namespace).values().forEach(result::putAll);
        return CompletableFuture.completedFuture(result);
    }

    @Override
    protected CompletableFuture<byte[]> removeRaw(String namespace, String id) {
        validateNamespace(namespace);
        return CompletableFuture.completedFuture(storage.get(namespace).get(hash(namespace, id)).remove(id));
    }

    @Override
    protected CompletableFuture<Map<String, byte[]>> getPartitionRaw(String namespace, int partition) {
        validateNamespace(namespace);
        validatePartition(namespace, partition);
        return CompletableFuture.completedFuture(new HashMap<>(storage.get(namespace).get(partition)));
    }

    @Override
    public CompletableFuture<Boolean> wipe() {
        initializeStorage();
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Boolean> clear(String namespace) {
        validateNamespace(namespace);
        storage.put(namespace, emptyPartitions(namespace));
        return SUCCESS;
    }

    @Override
    public CompletableFuture<Boolean> clear(String namespace, Set<String> ids) {
        validateNamespace(namespace);
        if (ids == null) {
            return SUCCESS;
        }
        Map<Integer, Map<String, byte[]>> data = storage.get(namespace);
        int count = partitions.get(namespace);
        for (String id : ids) {
            data.get(hash(id, count)).remove(id);
        }
        return SUCCESS;
    }

    @Override
    public int numberOfPartitions(String namespace) {
        validateNamespace(namespace);
        return partitions.get(namespace);
    }

    @Override
    public CompletableFuture<Boolean> clear(String namespace, int partition) {
        return super.clear(namespace, partition);
    }

    @Override
    public CompletableFuture<Boolean> repartition(String namespace, int newPartitionCount) {
        if (newPartitionCount < 0) {
            throw new IllegalArgumentException("New partition count must be positive!");
        }
        partitions.put(namespace, newPartitionCount);
        storage.put(namespace, repartition(namespace, storage.get(namespace).values()));
        return SUCCESS;
    }

    @Override
    protected String getDefaultNamespace() {
        return defaultNamespace;
    }

    private Map<Integer, Map<String, byte[]>> repartition(String namespace, Collection<Map<String, byte[]>> oldPartitions) {
        Map<Integer, Map<String, byte[]>> data = emptyPartitions(namespace);
        int count = partitions.get(namespace);
        for (Map<String, byte[]> partition : oldPartitions) {
            for (Map.Entry<String, byte[]> entry : partition.entrySet()) {
                String key = entry.getKey();
                data.get(hash(key, count)).put(key, entry.getValue());
            }
        }
        return data;
    }

    private int hash(String namespace, String key) {
        int numberOfPartitions = partitions.get(namespace);
        return hash(key, numberOfPartitions);
    }

    /**
     * Exposed for use by {@link Criteria}. Validates and throws if the given namespace is not a valid namespace.
     *
     * @param namespace The namespace to validate.
     * @throws IllegalArgumentException if the namespace is not a valid namespace.
     */
    void validateNamespace(String namespace) {
        if (!namespaces.contains(namespace)) {
            log.error("Namespace {} is not one of {}", namespace, namespaces);
            throw new IllegalArgumentException("The provided namespace is not a valid namespace: " + namespace);
        }
    }

    private void validatePartition(String namespace, int partition) {
        Integer count = partitions.get(namespace);
        if (partition >= count) {
            log.error("Partition {} is not between 0 and {} exclusive for {}", partition, count, namespace);
            throw new IllegalArgumentException("The provided partition is not valid: " + partition);
        }
    }

    private void initializeStorage() {
        storage = new HashMap<>();
        namespaces.forEach(namespace -> storage.put(namespace, emptyPartitions(namespace)));
    }

    private Map<Integer, Map<String, byte[]>> emptyPartitions(String namespace) {
        int count = partitions.get(namespace);
        Map<Integer, Map<String, byte[]>> emptyPartitions = new HashMap<>();
        IntStream.range(0, count).forEach(i -> emptyPartitions.put(i, new HashMap<>()));
        return emptyPartitions;
    }
}
