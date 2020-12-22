/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.storage;

import com.yahoo.bullet.common.BulletConfig;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * This augments the {@link BaseStorageManager} to allow storing Strings in addition by adding String versions of
 * accessors. In particular, data is encoded to and from {@link StandardCharsets#UTF_8} when working with byte[].
 */
abstract class BaseStringStorageManager<V extends Serializable> extends BaseStorageManager<V> implements Serializable  {
    private static final long serialVersionUID = 4030708385388045963L;

    /**
     * Constructor that takes a {@link BulletConfig}.
     *
     * @param config The config to use.
     */
    BaseStringStorageManager(BulletConfig config) {
        super(config);
    }

    /**
     * Stores a String for a given String identifier in the given namespace.
     *
     * @param namespace The namespace to store this value under.
     * @param id The ID to store this value under.
     * @param value The object to store as the value.
     * @return {@link CompletableFuture} that resolves to true the store succeeded.
     */
    public CompletableFuture<Boolean> putString(String namespace, String id, String value) {
        return putRaw(namespace, id, toBytes(value));
    }

    /**
     * Retrieves data stored for a given String identifier in the given namespace as a String.
     *
     * @param namespace The namespace of the data.
     * @param id The ID of the data.
     * @return {@link CompletableFuture} that resolves to the data.
     */
    public CompletableFuture<String> getString(String namespace, String id) {
        return getRaw(namespace, id).thenApply(BaseStringStorageManager::toString);
    }

    /**
     * Stores a map of IDs and String values into the storage in the given namespace.
     *
     * @param namespace The namespace to store the entries in.
     * @param data The map of IDs and values to store.
     * @return A {@link CompletableFuture} that resolves to true if the storage was completely successful.
     */
    public CompletableFuture<Boolean> putAllString(String namespace, Map<String, String> data) {
        return putAllRaw(namespace, toByteArrayMap(data));
    }

    /**
     * Retrieves all the IDs from the given namespace in the storage as a {@link Map} of IDs to their String values.
     *
     * @param namespace The namespace to retrieve from.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their String values.
     */
    public CompletableFuture<Map<String, String>> getAllString(String namespace) {
        return getAllRaw(namespace).thenApply(BaseStringStorageManager::toStringMap);
    }

    /**
     * Retrieves the values for the provided IDs from the given namespace in the storage as a {@link Map} of IDs to
     * their String values.
     *
     * @param namespace The namespace of the IDs.
     * @param ids The {@link Set} of IDs to retrieve.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their String values.
     */
    public CompletableFuture<Map<String, String>> getAllString(String namespace, Set<String> ids) {
        return getAllRaw(namespace, ids).thenApply(BaseStringStorageManager::toStringMap);
    }

    /**
     * Retrieves and removes data stored for a given String identifier as a String in the given namespace.
     *
     * @param namespace The namespace of the data.
     * @param id The ID of the data.
     * @return {@link CompletableFuture} that resolves to the data.
     */
    public CompletableFuture<String> removeString(String namespace, String id) {
        return removeRaw(namespace, id).thenApply(BaseStringStorageManager::toString);
    }

    /**
     * Retrieves the IDs stored in the provided partition for the given namespace. By default, if the storage manager
     * is unpartitioned, this works the same as {@link #getAll(String)}
     *
     * @param namespace The namespace to retrieve the IDs from.
     * @param partition The partition number to return.
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values as Strings
     *         or null if no data is present.
     */
    public CompletableFuture<Map<String, String>> getPartitionString(String namespace, int partition) {
        return getPartitionRaw(namespace, partition).thenApply(BaseStringStorageManager::toStringMap);
    }

    /**
     * Converts a {@link String} to a byte[].
     *
     * @param input The String input.
     * @return The byte[] encoding of the input.
     */
    public static byte[] toBytes(String input) {
        return input == null ? null : input.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Converts a byte[] input to a String.
     *
     * @param input The byte[] input.
     * @return The String decoded from the byte[].
     */
    public static String toString(byte[] input) {
        return input == null ? null : new String(input, StandardCharsets.UTF_8);
    }

    /**
     * Converts a {@link Map} of String to byte[] to a {@link Map} of String to String.
     *
     * @param input The String to byte[] map.
     * @return The String to String converted map.
     */
    public static Map<String, String> toStringMap(Map<String, byte[]> input) {
        if (input == null) {
            return null;
        }
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, byte[]> entry : input.entrySet()) {
            map.put(entry.getKey(), toString(entry.getValue()));
        }
        return map;
    }

    /**
     * Converts a {@link Map} of String to byte[] to a {@link Map} of String to String.
     *
     * @param input The String to byte[] map.
     * @return The String to String converted map.
     */
    public static Map<String, byte[]> toByteArrayMap(Map<String, String> input) {
        if (input == null) {
            return null;
        }
        Map<String, byte[]> map = new HashMap<>();
        for (Map.Entry<String, String> entry : input.entrySet()) {
            map.put(entry.getKey(), toBytes(entry.getValue()));
        }
        return map;
    }
}
