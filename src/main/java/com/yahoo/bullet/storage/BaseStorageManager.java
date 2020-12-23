/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.SerializerDeserializer;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.yahoo.bullet.common.Utilities.putNotNull;

@Slf4j
abstract class BaseStorageManager<V extends Serializable> implements AutoCloseable, Serializable {
    private static final long serialVersionUID = 951633252390860251L;
    static final CompletableFuture<Boolean> SUCCESS = CompletableFuture.completedFuture(true);

    protected BulletConfig config;

    /**
     * Constructor that takes a {@link BulletConfig}.
     *
     * @param config The config to use.
     */
    BaseStorageManager(BulletConfig config) {
        this.config = config;
    }

    /**
     * {@inheritDoc}. By default, close does nothing. If the particular storage needs to release resources, this is
     * the place to do it.
     */
    @Override
    public void close() {
    }

    // Accessors

    /**
     * Store a given ID and value for that ID into the given namespace in the storage.
     *
     * @param namespace The namespace to store the entry in.
     * @param id The unique ID to represent this entry.
     * @param value The value to store for this entry.
     * @return A {@link CompletableFuture} that resolves to true if the storage was successful.
     */
    protected abstract CompletableFuture<Boolean> putRaw(String namespace, String id, byte[] value);

    /**
     * Retrieves a given ID from the given namespace in the storage.
     *
     * @param namespace The namespace to retrieve from.
     * @param id The unique ID to retrieve from the storage.
     * @return A {@link CompletableFuture} that resolves to the byte[] value for this ID or null if it does not exist.
     */
    protected abstract CompletableFuture<byte[]> getRaw(String namespace, String id);

    /**
     * Stores a map of IDs and values into the storage in the given namespace. By default, calls
     * {@link #putRaw(String, String, byte[])} in parallel.
     *
     * @param namespace The namespace to store the entries in.
     * @param data The map of IDs and values to store.
     * @return A {@link CompletableFuture} that resolves to true if the storage was completely successful.
     */
    protected CompletableFuture<Boolean> putAllRaw(String namespace, Map<String, byte[]> data) {
        if (data == null) {
            return SUCCESS;
        }
        int i = 0;
        CompletableFuture[] futures = new CompletableFuture[data.size()];
        for (Map.Entry<String, byte[]> entry : data.entrySet()) {
            futures[i] = putRaw(namespace, entry.getKey(), entry.getValue());
            i++;
        }
        return CompletableFuture.allOf(futures).thenApply(ignored -> true);
    }

    /**
     * Retrieves all the IDs from the given namespace in the storage as byte[].
     *
     * @param namespace The namespace to retrieve from.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values as byte[].
     */
    protected abstract CompletableFuture<Map<String, byte[]>> getAllRaw(String namespace);

    /**
     * Retrieves the values for the provided IDs from the given namespace in the storage as byte[]. By default, calls
     * {@link #getRaw(String, String)} repeatedly on each of the given IDs in parallel.
     *
     * @param namespace The namespace of the IDs.
     * @param ids The {@link Set} of IDs to retrieve.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values as byte[].
     */
    protected CompletableFuture<Map<String, byte[]>> getAllRaw(String namespace, Set<String> ids) {
        if (ids == null) {
            return CompletableFuture.completedFuture(null);
        }
        ConcurrentHashMap<String, byte[]> data = new ConcurrentHashMap<>();

        int i = 0;
        CompletableFuture[] futures = new CompletableFuture[ids.size()];
        for (String id : ids) {
            futures[i] = getRaw(namespace, id).thenAccept(v -> putNotNull(data, id, v));
            i++;
        }
        return CompletableFuture.allOf(futures).thenApply(ignored -> data);
    }

    /**
     * Removes a given ID from the given namespace in the storage.
     *
     * @param namespace The namespace of the ID.
     * @param id The unique ID to remove from the storage.
     * @return A {@link CompletableFuture} that resolves to the byte[] value for this ID or null if it does not exist.
     */
    protected abstract CompletableFuture<byte[]> removeRaw(String namespace, String id);

    /**
     * Removes all the IDs from the storage across all namespaces.
     *
     * @return A {@link CompletableFuture} that resolves to true if the wipe was successful.
     */
    public abstract CompletableFuture<Boolean> wipe();

    /**
     * Clears the specified namespace.
     *
     * @param namespace The namespace to clear.
     * @return A {@link CompletableFuture} that resolves to true if the wipe was successful.
     */
    public abstract CompletableFuture<Boolean> clear(String namespace);

    /**
     * Removes a given set of IDs from the storage under the given namespace.
     *
     * @param namespace The namespace that has these IDs.
     * @param ids The set of IDs to remove from the storage for the given namespace.
     * @return A {@link CompletableFuture} that resolves to true if the wipe was successful and throws otherwise.
     */
    public abstract CompletableFuture<Boolean> clear(String namespace, Set<String> ids);

    /**
     * Stores any {@link Serializable} object for a given String identifier in the given namespace.
     *
     * @param namespace The namespace to store this value under.
     * @param id The ID to store this value under.
     * @param value The object to store as the value.
     * @return A {@link CompletableFuture} that resolves to true if the store succeeded.
     */
    public CompletableFuture<Boolean> put(String namespace, String id, V value) {
        return putRaw(namespace, id, this.convert(value));
    }

    /**
     * Retrieves data stored for a given String identifier in the given namespace as a {@link Serializable} object.
     *
     * @param namespace The namespace of the data.
     * @param id The ID of the data.
     * @return A {@link CompletableFuture} that resolves to the data.
     */
    public CompletableFuture<V> get(String namespace, String id) {
        return getRaw(namespace, id).thenApply(this::convert);
    }

    /**
     * Stores a map of IDs and values into the storage in the given namespace.
     *
     * @param namespace The namespace to store the entries in.
     * @param data The map of IDs and values to store.
     * @return A {@link CompletableFuture} that resolves to true if the storage was completely successful.
     */
    public CompletableFuture<Boolean> putAll(String namespace, Map<String, V> data) {
        return putAllRaw(namespace, fromObjectMap(data, this::convert));
    }

    /**
     * Retrieves all the IDs from the given namespace in the storage as a {@link Map} of IDs to their
     * {@link Serializable} values.
     *
     * @param namespace The namespace to retrieve from.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values.
     */
    public CompletableFuture<Map<String, V>> getAll(String namespace) {
        return getAllRaw(namespace).thenApply(m -> toObjectMap(m, this::convert));
    }

    /**
     * Retrieves the values for the provided IDs from the given namespace in the storage as a {@link Map} of IDs to
     * their {@link Serializable} values.
     *
     * @param namespace The namespace of the IDs.
     * @param ids The {@link Set} of IDs to retrieve.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values.
     */
    public CompletableFuture<Map<String, V>> getAll(String namespace, Set<String> ids) {
        return getAllRaw(namespace, ids).thenApply(m -> toObjectMap(m, this::convert));
    }

    /**
     * Retrieves and removes data stored for a given String identifier as a {@link Serializable} object in the given
     * namespace.
     *
     * @param namespace The namespace of the data.
     * @param id The ID of the data.
     * @return A {@link CompletableFuture} that resolves to the data.
     */
    public CompletableFuture<V> remove(String namespace, String id) {
        return removeRaw(namespace, id).thenApply(this::convert);
    }

    // Partition methods

    /**
     * Returns the number of partitions stored in this storage manager for the given namespace. Partitions can be
     * sharded in storage and can also be used as a smaller unit of processing to reduce memory requirements. Partitions
     * are numbered with integers from 0 inclusive to numberOfPartitions(String) exclusive for the namespace. By
     * default, if the storage manager is unpartitioned, this returns 0.
     *
     * @param namespace The namespace whose partitions are being asked for.
     * @return The number of partitions in this storage manager.
     */
    public int numberOfPartitions(String namespace) {
        return 0;
    }

    /**
     * Retrieves the IDs stored in the provided partition for the given namespace. By default, if the storage manager
     * is unpartitioned, this works the same as {@link #getAllRaw(String)}
     *
     * @param namespace The namespace to retrieve the IDs from.
     * @param partition The partition number to return.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values as byte
     *         arrays or null if no data is present.
     */
    protected CompletableFuture<Map<String, byte[]>> getPartitionRaw(String namespace, int partition) {
        return getAllRaw(namespace);
    }

    /**
     * Retrieves the IDs stored in the provided partition for the given namespace. By default, if the storage manager
     * is unpartitioned, this works the same as {@link #getAll(String)}
     *
     * @param namespace The namespace to retrieve the IDs from.
     * @param partition The partition number to return.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values as Serializable
     *         objects or null if no data is present.
     */
    public CompletableFuture<Map<String, V>> getPartition(String namespace, int partition) {
        return getPartitionRaw(namespace, partition).thenApply(d -> BaseStorageManager.toObjectMap(d, this::convert));
    }

    /**
     * Clears the specified partition under the given namespace.
     *
     * @param namespace The namespace for the partition.
     * @param partition The partition to clear.
     * @return A {@link CompletableFuture} that resolves to true if the wipe was successful.
     */
    public CompletableFuture<Boolean> clear(String namespace, int partition) {
        return clear(namespace);
    }

    /**
     * Repartitions the data into the given new number of partitions for the given namespace. If the storage manager
     * is unpartitioned, this does nothing.
     *
     * @param namespace The namespace to repartition.
     * @param newPartitionCount The new number of partitions to use.
     * @return A {@link CompletableFuture} that resolves to true if the repartitioning was successful.
     */
    public CompletableFuture<Boolean> repartition(String namespace, int newPartitionCount) {
        return SUCCESS;
    }

    // Conversion methods

    /**
     * Converts a @{@link byte[]} to a type of the given object. By default, uses {@link SerializerDeserializer} to
     * deserialize using Java deserialization.
     *
     * @param bytes The byte[] to convert.
     * @return The converted object or null if the input was null or the conversion was unable to be performed.
     */
    protected V convert(byte[] bytes) {
        // While SerializerDeserializer handles nulls, adding a null check to avoid using exceptions for control flow
        return bytes == null ? null : SerializerDeserializer.fromBytes(bytes);
    }

    /**
     * Converts an object of the given type to a @{@link byte[]}. By default, uses {@link SerializerDeserializer} to
     * serialize using Java serialization.
     *
     * @param object The object to convert.
     * @return The converted byte[] or null if the input was null or the conversion was unable to be performed.
     */
    protected byte[] convert(V object) {
        // While SerializerDeserializer handles nulls, adding a null check to avoid using exceptions for control flow
        return object == null ? null : SerializerDeserializer.toBytes(object);
    }

    /**
     * Converts a {@link Map} of String and the given type to a {@link Map} of String to byte[].
     *
     * @param input The String to the given type map.
     * @param converter The {@link Function} that converts from the given type to byte[].
     * @param <S> The type to convert from.
     * @return The String to byte[] converted map.
     */
    public static <S> Map<String, byte[]> fromObjectMap(Map<String, S> input, Function<S, byte[]> converter) {
        if (input == null) {
            return null;
        }
        Map<String, byte[]> map = new HashMap<>();
        for (Map.Entry<String, S> entry : input.entrySet()) {
            map.put(entry.getKey(), converter.apply(entry.getValue()));
        }
        return map;
    }

    /**
     * Converts a {@link Map} of String to byte[] to a {@link Map} of String to the given type.
     *
     * @param input The String to byte[] map.
     * @param converter The {@link Function} that converts from byte[] to the given type.
     * @param <S> The type to convert to.
     * @return The String to the given type converted map.
     */
    public static <S> Map<String, S> toObjectMap(Map<String, byte[]> input, Function<byte[], S> converter) {
        if (input == null) {
            return null;
        }
        Map<String, S> map = new HashMap<>();
        for (Map.Entry<String, byte[]> entry : input.entrySet()) {
            map.put(entry.getKey(), converter.apply(entry.getValue()));
        }
        return map;
    }
}
