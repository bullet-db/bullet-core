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
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Slf4j
public abstract class StorageManager implements AutoCloseable, Serializable {
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
     * Exposed for testing. Converts a @{@link byte[]} to a type of the given object.
     *
     * @param bytes The byte[] to convert.
     * @param <U> The type of the object to convert it to.
     * @return The converted object or null if the input was null or the conversion was unable to be performed.
     */
    @SuppressWarnings("unchecked")
    static <U> U convert(byte[] bytes) {
        // While SerializerDeserializer handles nulls, adding a null check to avoid using exceptions for control flow
        return bytes == null ? null : SerializerDeserializer.fromBytes(bytes);
    }

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
     * Retrieves all the IDs stored with {@link #putString(String, String)} or {@link #put(String, byte[])} or
     * {@link #putObject(String, Serializable)} from the storage as Strings.
     *
     * @return A {@link CompletableFuture} that resolves to a {@link Map} of IDs to their stored values as Strings or
     *         null if no data is present.
     */
    public CompletableFuture<Map<String, String>> getAllString() {
        return getAll().thenApplyAsync(StorageManager::toStringMap);
    }

    /**
     * Retrieves and removes data stored for a given String identifier as a {@link Serializable} object.
     *
     * @param id The id of the data.
     * @param <U> The type of the {@link Serializable} object.
     * @return {@link CompletableFuture} that resolves to the data, null if no data, or completes exceptionally.
     */
    public <U extends Serializable> CompletableFuture<U> removeObject(String id) {
        return remove(id).thenApplyAsync(StorageManager::convert);
    }

    /**
     * Stores any {@link Serializable} object for a given String identifier.
     *
     * @param id The id to store this data under.
     * @param data The object to store as the data.
     * @param <U> The type of the {@link Serializable} object.
     * @return {@link CompletableFuture} that resolves to true the store succeeded or false if not.
     */
    public <U extends Serializable> CompletableFuture<Boolean> putObject(String id, U data) {
        return put(id, SerializerDeserializer.toBytes(data));
    }

    /**
     * Retrieves data stored for a given String identifier as a {@link Serializable} object.
     *
     * @param id The id of the data.
     * @param <U> The type of the {@link Serializable} object.
     * @return {@link CompletableFuture} that resolves to the data, null if no data, or completes exceptionally.
     *
     */
    public <U extends Serializable> CompletableFuture<U> getObject(String id) {
        return get(id).thenApplyAsync(StorageManager::convert);
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
     * Removes all the IDs stored with {@link #putString(String, String)} or {@link #put(String, byte[])} or
     * {@link #putObject(String, Serializable)} from the storage as byte[].
     *
     * @return A {@link CompletableFuture} that resolves to true or false if the wipe was successful.
     */
    public abstract CompletableFuture<Boolean> clear();

    /**
     * Removes a given set of IDs from the storage.
     *
     * @param ids The set of IDs to remove from the storage.
     * @return A {@link CompletableFuture} that resolves to true if the wipe was successful and throws otherwise.
     */
    public abstract CompletableFuture<Boolean> clear(Set<String> ids);

    @Override
    public void close() {
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
}
