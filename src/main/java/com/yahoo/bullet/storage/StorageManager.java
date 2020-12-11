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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * @param <V> The type of the {@link Serializable} data stored in the underlying storage.
 *
 * Encapsulates the concept of a Storage. It can abstract away a key-value storage as well as other kinds of storing
 * data. It lets you store raw byte[], Strings or objects of the type parameter. Implementors of this class are advised
 * to use the {@link #convert(Serializable)} and {@link #convert(byte[])} to go back and forth between the actual data
 * and implement the methods that operate on byte[]. The String methods are provided so that storing String instead of
 * byte[] is supported by all Storages.
 *
 * Note that {@link #getAll()} method is only provided for the raw byte[] access. Use {@link #getAllObjects(Criteria)}
 * or {@link #getAllStrings(Criteria)} for object and String access using {@link Criteria} if needed. There is also a
 * {@link #getAll(Criteria)} for the raw byte[] access using {@link Criteria}.
 *
 * It exposes these optional concepts:
 * 1. The concept of a namespace (by default, assumes there is only one default namespace). This can be used to abstract
 *    different concepts like tables if the storage is relational or multiple conceptual storages. If you need
 *    namespaces for your storage, implement the namespace switching method {@link #use(String)}.
 * 2. The concept of a partition (by default, assumes the storage is unpartitioned). It is upto the concrete
 *    implementation to choose to shard its data if needed and understand how to access its shards given the keys. A
 *    partition is defined at each namespace level. If you need partitions for your storage, implement the partition
 *    specific methods - {@link #getPartition(int)}, {@link #clear(int)} and {@link #numberOfPartitions()}.
 */
@Slf4j
public abstract class StorageManager<V extends Serializable> implements AutoCloseable, Serializable {
    private static final long serialVersionUID = 6384361951608923687L;

    protected BulletConfig config;

    /**
     * Constructor that takes a {@link BulletConfig}.
     *
     * @param config The config to use.
     */
    public StorageManager(BulletConfig config) {
        this.config = config;
    }

    /**
     * Create a StorageManager instance using the class specified in the config file.
     *
     * @param config The non-null {@link BulletConfig} containing the class name and StorageManager settings.
     * @return an instance of specified class initialized with settings from the input file and defaults.
     */
    public static StorageManager from(BulletConfig config) {
        try {
            return config.loadConfiguredClass(BulletConfig.STORAGE_CLASS_NAME);
        } catch (RuntimeException e) {
            throw new RuntimeException("Cannot create StorageManager instance.", e.getCause());
        }
    }

    /**
     * {@inheritDoc}. By default, close does nothing. If the particular storage needs to release resources, this is
     * the place to do it.
     */
    @Override
    public void close() {
    }

    // Object methods

    /**
     * Retrieves and removes data stored for a given String identifier as a {@link Serializable} object.
     *
     * @param id The ID of the data.
     * @return {@link CompletableFuture} that resolves to the data, null if no data, or completes exceptionally.
     */
    public CompletableFuture<V> removeObject(String id) {
        return remove(id).thenApplyAsync(this::convert);
    }

    /**
     * Stores any {@link Serializable} object for a given String identifier.
     *
     * @param id The ID to store this data under.
     * @param data The object to store as the data.
     * @return {@link CompletableFuture} that resolves to true the store succeeded or false if not.
     */
    public CompletableFuture<Boolean> putObject(String id, V data) {
        return put(id, this.convert(data));
    }

    /**
     * Retrieves data stored for a given String identifier as a {@link Serializable} object.
     *
     * @param id The ID of the data.
     * @return {@link CompletableFuture} that resolves to the data, null if no data, or completes exceptionally.
     *
     */
    public CompletableFuture<V> getObject(String id) {
        return get(id).thenApplyAsync(this::convert);
    }

    /**
     * Retrieves all the IDs matching the specified {@link Criteria} stored with {@link #putString(String, String)} or
     * {@link #put(String, byte[])} or {@link #putObject(String, Serializable)} from the storage as Strings.
     *
     * @param criteria The {@link Criteria} understood by this storage.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values or
     *         null if no data is present. It completes exceptionally if there were issues.
     */
    public CompletableFuture<Map<String, V>> getAllObjects(Criteria criteria) {
        return getAll(criteria).thenApplyAsync(m -> toObjectMap(m, this::convert));
    }

    /**
     * Retrieves a given ID from the storage stored using {@link #put(String, byte[])}.
     *
     * @param id The unique ID to retrieve from the storage.
     * @return A {@link CompletableFuture} that resolves to the stored value for this ID or null if it does not exist
     *         as a byte[]. It completes exceptionally if there were issues.
     */
    public abstract CompletableFuture<byte[]> get(String id);

    /**
     * Removes a given ID from the storage stored using {@link #put(String, byte[])}.
     *
     * @param id The unique ID to remove from the storage.
     * @return A {@link CompletableFuture} that resolves to the stored value for this ID or null if it does not exist
     *         as a byte[]. It completes exceptionally if there were issues.
     */
    public abstract CompletableFuture<byte[]> remove(String id);

    /**
     * Store a given ID and value for that ID into the storage.
     *
     * @param id The unique ID to represent this entry.
     * @param value The value to store for this entry.
     * @return A {@link CompletableFuture} that resolves to true if the storage was successful.
     */
    public abstract CompletableFuture<Boolean> put(String id, byte[] value);

    /**
     * Stores a map of IDs and values into the storage.
     *
     * @param data The map of IDs and values to store.
     * @return A {@link CompletableFuture} that resolves to true if the storage was completely successful.
     */
    public abstract CompletableFuture<Boolean> putAll(Map<String, byte[]> data);

    /**
     * Retrieves all the IDs stored with {@link #putString(String, String)} or {@link #put(String, byte[])} or
     * {@link #putObject(String, Serializable)} from the storage as byte[].
     *
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values as byte[] or
     *         null if no data is present. It completes exceptionally if there were issues.
     */
    public abstract CompletableFuture<Map<String, byte[]>> getAll();

    /**
     * Retrieves the values for the provided IDs stored with {@link #putString(String, String)} or
     * {@link #put(String, byte[])} or {@link #putObject(String, Serializable)} from the storage as byte[].
     *
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values as byte[] or
     *         null if no data is present. It completes exceptionally if there were issues.
     */
    public abstract CompletableFuture<Map<String, byte[]>> getAll(Set<String> ids);

    /**
     * Retrieves all the IDs matching the specified {@link Criteria} stored with {@link #putString(String, String)} or
     * {@link #put(String, byte[])} or {@link #putObject(String, Serializable)} from the storage as byte[].
     *
     * @param criteria The {@link Criteria} understood by this storage.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values as byte[] or
     *         null if no data is present. It completes exceptionally if there were issues.
     */
    public CompletableFuture<Map<String, byte[]>> getAll(Criteria criteria) {
        Objects.requireNonNull(criteria);
        return criteria.retrieve(this);
    }

    /**
     * Removes all the IDs stored with {@link #putString(String, String)} or {@link #put(String, byte[])} or
     * {@link #putObject(String, Serializable)} from the storage as byte[].
     *
     * @return A {@link CompletableFuture} that resolves to true or false if the wipe was successful.
     */
    public abstract CompletableFuture<Boolean> clear();

    /**
     * Removes a given set of IDs from the storage under the current namespace.
     *
     * @param ids The set of IDs to remove from the storage.
     * @return A {@link CompletableFuture} that resolves to true if the wipe was successful and throws otherwise.
     */
    public abstract CompletableFuture<Boolean> clear(Set<String> ids);

    // Namespace methods

    /**
     * The interface to switch namespaces. By default, does nothing.
     *
     * @param namespace The new namespace to use.
     */
    public void use(String namespace) {
    }

    /**
     * The current namespace used by the storage. By default, returns null.
     *
     * @return The current namespace of the storage.
     */
    public String getNamespace() {
        return null;
    }

    /**
     * Clears the specified namespace. By default, since the StorageManager has one namespace, this works the same as
     * {@link #clear()}.
     *
     * @param namespace The namespace to clear.
     * @return A {@link CompletableFuture} that resolves to true if the wipe was successful and throws otherwise.
     */
    public CompletableFuture<Boolean> clear(String namespace) {
        return clear();
    }

    // Partition methods

    /**
     * Returns the number of partitions stored in this StorageManager. Partitions can be sharded in storage and can
     * also be used as a smaller unit of processing to reduce memory requirements. Partitions are numbered with
     * integers from 0 inclusive to numberOfPartitions() exclusive. By default, the StorageManager is unpartitioned.
     *
     * @return The number of partitions in this PartitionedStorageManager.
     */
    public int numberOfPartitions() {
        return 0;
    }

    /**
     * Retrieves the IDs stored in the provided partition. By default, since the StorageManager is unpartitioned, this
     * works the same as {@link #getAll()}.
     *
     * @param partition The partition number to return.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values as byte
     *         arrays or null if no data is present.
     */
    public CompletableFuture<Map<String, byte[]>> getPartition(int partition) {
        return getAll();
    }

    /**
     * Clears the specified partition. By default, since the StorageManager is unpartitioned, this works the same as
     * {@link #clear()}.
     *
     * @param partition The partition to clear.
     * @return A {@link CompletableFuture} that resolves to true if the wipe was successful and throws otherwise.
     */
    public CompletableFuture<Boolean> clear(int partition) {
        return clear();
    }

    // String methods

    /**
     * Retrieves a given ID from the storage stored using {@link #putString(String, String)} or
     * {@link #put(String, byte[])} as a String.
     *
     * @param id The unique ID to retrieve from the storage.
     * @return A {@link CompletableFuture} that resolves to the String version of the stored value for this ID or null
     *         if it does not exist.
     */
    public CompletableFuture<String> getString(String id) {
        return get(id).thenApplyAsync(StorageManager::toString);
    }

    /**
     * Removes a given ID from the storage stored using {@link #putString(String, String)} or
     * {@link #put(String, byte[])} as a String.
     *
     * @param id The unique ID to remove from the storage.
     * @return A {@link CompletableFuture} that resolves to the String version of the stored value for this ID or null
     *         if it does not exist.
     */
    public CompletableFuture<String> removeString(String id) {
        return remove(id).thenApplyAsync(StorageManager::toString);
    }

    /**
     * Store a given ID and String value for that ID into the storage.
     *
     * @param id The unique ID to represent this entry.
     * @param value The String value to store for this entry.
     * @return A {@link CompletableFuture} that resolves to true or false if the storage was successful.
     */
    public CompletableFuture<Boolean> putString(String id, String value) {
        return put(id, toBytes(value));
    }

    /**
     * Retrieves all the IDs matching the specified {@link Criteria} stored with {@link #putString(String, String)} or
     * {@link #put(String, byte[])} or {@link #putObject(String, Serializable)} from the storage as Strings.
     *
     * @param criteria The {@link Criteria} understood by this storage.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values as String or
     *         null if no data is present. It completes exceptionally if there were issues.
     */
    public CompletableFuture<Map<String, String>> getAllStrings(Criteria criteria) {
        return getAll(criteria).thenApplyAsync(StorageManager::toStringMap);
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
     * Exposed for testing. Helper to convert a {@link String} to a byte[].
     *
     * @param input The String input.
     * @return The byte[] encoding of the input.
     */
    static byte[] toBytes(String input) {
        return input == null ? null : input.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Exposed for testing. Converts a byte[] input to a String.
     *
     * @param input The byte[] input.
     * @return The String decoded from the byte[].
     */
    static String toString(byte[] input) {
        return input == null ? null : new String(input, StandardCharsets.UTF_8);
    }

    /**
     * Exposed for testing. Converts a {@link Map} of String to byte[] to a {@link Map} of String to String.
     *
     * @param input The String to byte[] map.
     * @return The String to String converted map.
     */
    static Map<String, String> toStringMap(Map<String, byte[]> input) {
        return toObjectMap(input, StorageManager::toString);
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
