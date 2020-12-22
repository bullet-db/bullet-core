/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import com.yahoo.bullet.common.BulletConfig;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * @param <V> The type of the {@link Serializable} data stored in the underlying storage.
 *
 * Encapsulates the concept of a Storage. It can abstract away a key-value storage as well as other kinds of storing
 * data. It uses three main concepts (see below) - namespaces, partitions and criteria to abstract away most storages.
 * It lets you store and retrieve raw byte[] or strings, or objects of the type parameter.
 *
 * It exposes these concepts:
 * <ol>
 *   <li>
 *   The concept of a namespace. This can be used to abstract different concepts like tables if the storage is
 *   relational or multiple conceptual storages.
 *   </li>
 *   <li>
 *   The concept of a partition. It is upto the concrete implementation to choose to shard its data if needed and
 *   understand how to access its shards given the keys. A partition is defined at each namespace level. If you need
 *   partitions for your storage, override the partition specific methods - {@link #getPartition(String, int)},
 *   {@link #clear(String, int)}, {@link #numberOfPartitions(String)} and {@link #repartition(String, int)}, as well
 *   actually partitioning the data when implementing the byte[] methods. For convenience, a {@link #hash(String, int)}
 *   is provided.
 *   </li>
 *   <li>
 *   The concept of a criteria. A criteria is a general query. A specific {@link StorageManager} can provide storage
 *   specific {@link Criteria} for storage specific querying needs. Note that the {@link Criteria} methods,
 *   {@link #retrieveAll(Criteria)} and {@link #getAll(Criteria)} do not take a namespace since those are handled by
 *   the {@link Criteria} implementations. The {@link #apply(Criteria, Object)} can be used to apply changes to the
 *   storage as well.
 *   </li>
 * </ol>
 *
 * Implementors of this class should implement the various raw byte[] methods - the accessors as well as the clear
 * methods. To change the default conversion to and from the type of the StorageManager and the byte[], you can
 * override the {@link #convert(Serializable)} and {@link #convert(byte[])}.
 *
 *  For convenience,
 * <ol>
 *   <li>
 *   All the accessors are provided with their String variants to store and retrieve values as Strings
 *   in addition to the the type of data stored in the storage. If you wish to control how the encoding happens, you
 *   may override the {@link #toString(byte[])}, {@link #toBytes(String)}. The map flavors are available as
 *   {@link #toByteArrayMap(Map)} and {@link #toStringMap(Map)} methods.
 *   <li>
 *   All the accessors (including the ones in 1. above) are provided without requiring a namespace. These methods use
 *   the {@link #DEFAULT_NAMESPACE} when invoking the corresponding methods that do require a namespace. If you wish
 *   to control the default namespace used, override the {@link #getDefaultNamespace()} method.
 *   </li>
 * </ol>
 */
public abstract class StorageManager<V extends Serializable> extends BaseStringStorageManager<V> implements Serializable {
    private static final long serialVersionUID = -2521566298026119635L;
    
    public static final String DEFAULT_NAMESPACE = "";

    /**
     * Constructor that takes a {@link BulletConfig}.
     *
     * @param config The config to use.
     */
    public StorageManager(BulletConfig config) {
        super(config);
    }

    /**
     * Retrieves all the IDs matching the specified {@link Criteria} from the storage as the type of the storage.
     *
     * @param criteria The {@link Criteria} understood by this storage.
     * @param <T> The type of query taken by the {@link Criteria}.
     * @param <R> The type of result returned by the {@link Criteria}.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values.
     */
    public <T, R> CompletableFuture<Map<String, V>> getAll(Criteria<T, R> criteria) {
        return criteria.get(this);
    }

    /**
     * Retrieves all the data matching the specified {@link Criteria} as the types of the {@link Criteria}.
     *
     * @param criteria The {@link Criteria} understood by this storage.
     * @param <T> The type of query taken by the {@link Criteria}.
     * @param <R> The type of result returned by the {@link Criteria}.
     * @return A {@link CompletableFuture} that resolves to the type returned by the {@link Criteria}.
     */
    public <T, R> CompletableFuture<R> retrieveAll(Criteria<T, R> criteria) {
        return criteria.retrieve(this);
    }

    /**
     * Applies the given {@link Criteria} to this storage. It is upto the {@link Criteria} what it does.
     *
     * @param criteria The {@link Criteria} to apply.
     * @param query The query that the {@link Criteria} uses to apply.
     * @param <T> The type of query taken by the {@link Criteria}.
     * @param <R> The type of result returned by the {@link Criteria}.
     * @return A {@link CompletableFuture} that resolves to the type returned by the {@link Criteria}.
     */
    public <T, R> CompletableFuture<R> apply(Criteria<T, R> criteria, T query) {
        return criteria.apply(this, query);
    }

    /**
     * Gets the default namespace to store data under.
     * 
     * @return The default namespace.
     */
    protected String getDefaultNamespace() {
        return DEFAULT_NAMESPACE;
    }

    /**
     * Clears the default namespace.
     *
     * @return A {@link CompletableFuture} that resolves to true if the wipe was successful.
     */
    public CompletableFuture<Boolean> clear() {
        return clear(getDefaultNamespace());
    }

    /**
     * Clears the specified partition under the default namespace.
     *
     * @param partition The partition to clear.
     * @return A {@link CompletableFuture} that resolves to true if the wipe was successful.
     */
    public CompletableFuture<Boolean> clear(int partition) {
        return clear(getDefaultNamespace(), partition);
    }

    /**
     * Removes a given set of IDs from the storage under the default namespace.
     *
     * @param ids The set of IDs to remove from the storage for the default namespace.
     * @return A {@link CompletableFuture} that resolves to true if the wipe was successful.
     */
    public CompletableFuture<Boolean> clear(Set<String> ids) {
        return clear(getDefaultNamespace(), ids);
    }
    /**
     * Stores any {@link Serializable} object for a given String identifier in the default namespace.
     *
     * @param id The ID to store this data under.
     * @param data The object to store as the data.
     * @return {@link CompletableFuture} that resolves to true the store succeeded or false if not.
     */
    public CompletableFuture<Boolean> put(String id, V data) {
        return put(getDefaultNamespace(), id, data);
    }
    /**
     * Retrieves data stored for a given String identifier in the default namespace as a {@link Serializable} object.
     *
     * @param id The ID of the data.
     * @return {@link CompletableFuture} that resolves to the data.
     */
    public CompletableFuture<V> get(String id) {
        return get(getDefaultNamespace(), id);
    }
    /**
     * Stores a map of IDs and values into the storage in the default namespace.
     *
     * @param data The map of IDs and values to store.
     * @return A {@link CompletableFuture} that resolves to true if the storage was completely successful.
     */
    public CompletableFuture<Boolean> putAll(Map<String, V> data) {
        return putAll(getDefaultNamespace(), data);
    }

    /**
     * Retrieves all the IDs from the default namespace in the storage as a {@link Map} of IDs to their
     * {@link Serializable} values.
     *
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values.
     */
    public CompletableFuture<Map<String, V>> getAll() {
        return getAll(getDefaultNamespace());
    }

    /**
     * Retrieves the values for the provided IDs from the default namespace in the storage as a {@link Map} of IDs to
     * their {@link Serializable} values.
     *
     * @param ids The {@link Set} of IDs to retrieve.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values.
     */
    public CompletableFuture<Map<String, V>> getAll(Set<String> ids) {
        return getAll(getDefaultNamespace(), ids);
    }
    
    /**
     * Retrieves and removes data stored for a given String identifier as a {@link Serializable} object in the default
     * namespace.
     *
     * @param id The ID of the data.
     * @return {@link CompletableFuture} that resolves to the data.
     */
    public CompletableFuture<V> remove(String id) {
        return remove(getDefaultNamespace(), id);
    }

    /**
     * Returns the number of partitions stored in this storage manager for the default namespace. Partitions can be
     * sharded in storage and can also be used as a smaller unit of processing to reduce memory requirements. Partitions
     * are numbered with integers from 0 inclusive to numberOfPartitions() exclusive for the namespace. By
     * default, the storage manager is unpartitioned.
     *
     * @return The number of partitions in this storage manager.
     */
    public int numberOfPartitions() {
        return numberOfPartitions(getDefaultNamespace());
    }

    /**
     * Retrieves the IDs stored in the provided partition for the default namespace.
     *
     * @param partition The partition number to return.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values as byte
     *         arrays or null if no data is present.
     */
    public CompletableFuture<Map<String, V>> getPartition(int partition) {
        return getPartition(getDefaultNamespace(), partition);
    }

    /**
     * Repartitions the data into the given new number of partitions in the default namespace.
     *
     * @param newPartitionCount The new number of partitions to use.
     * @return A {@link CompletableFuture} that resolves to true if the repartitioning was successful.
     */
    public CompletableFuture<Boolean> repartition(int newPartitionCount) {
        return repartition(getDefaultNamespace(), newPartitionCount);
    }

    /**
     * Stores a String for a given String identifier in the default namespace.
     *
     * @param id The ID to store this value under.
     * @param value The object to store as the value.
     * @return {@link CompletableFuture} that resolves to true the store succeeded or false if not.
     */
    public CompletableFuture<Boolean> putString(String id, String value) {
        return putString(getDefaultNamespace(), id, value);
    }

    /**
     * Retrieves data stored for a given String identifier in the default namespace as a String.
     *
     * @param id The ID of the data.
     * @return {@link CompletableFuture} that resolves to the data.
     */
    public CompletableFuture<String> getString(String id) {
        return getString(getDefaultNamespace(), id);
    }

    /**
     * Stores a map of IDs and String values into the storage in the default namespace.
     *
     * @param data The map of IDs and values to store.
     * @return A {@link CompletableFuture} that resolves to true if the storage was completely successful.
     */
    public CompletableFuture<Boolean> putAllString(Map<String, String> data) {
        return putAllString(getDefaultNamespace(), data);
    }

    /**
     * Retrieves all the IDs from the default namespace in the storage as a {@link Map} of IDs to their String values.
     *
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their String values.
     */
    public CompletableFuture<Map<String, String>> getAllString() {
        return getAllString(getDefaultNamespace());
    }

    /**
     * Retrieves the values for the provided IDs from the default namespace in the storage as a {@link Map} of IDs to
     * their String values.
     *
     * @param ids The {@link Set} of IDs to retrieve.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their String values.
     */
    public CompletableFuture<Map<String, String>> getAllString(Set<String> ids) {
        return getAllString(getDefaultNamespace(), ids);
    }

    /**
     * Retrieves and removes data stored for a given String identifier as a String in the default namespace.
     *
     * @param id The ID of the data.
     * @return {@link CompletableFuture} that resolves to the data.
     */
    public CompletableFuture<String> removeString(String id) {
        return removeString(getDefaultNamespace(), id);
    }

    /**
     * A default hash function for a given String that places the String into 0 to numberOfPartitions exclusive. Note,
     * that numberOfPartitions must be a whole number.
     *
     * @param key The String key to hash.
     * @param numberOfPartitions The number of partitions. Must be zero or positive.
     * @return An integer from 0 to numberOfPartitions.
     */
    public static int hash(String key, int numberOfPartitions) {
        return Math.floorMod(key == null ? 42 : key.hashCode(), numberOfPartitions);
    }

    /**
     * Create a {@link StorageManager} instance using the class specified in the config file.
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
}
