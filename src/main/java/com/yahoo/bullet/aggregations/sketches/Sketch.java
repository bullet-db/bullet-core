/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.aggregations.sketches;

import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.result.Metadata;
import com.yahoo.bullet.result.Metadata.Concept;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * This class encapsulates a type of Sketch. Since one type of Sketch is used for updating and another for unioning,
 * this will encapsulate both of them and provide methods to serialize, union and collect results.
 */
public abstract class Sketch {
    protected boolean updated = false;
    protected boolean unioned = false;

    /**
     * Serializes the sketch.
     *
     * @return A byte[] representing the serialized sketch.
     */
    public abstract byte[] serialize();

    /**
     * Union a sketch serialized using {@link #serialize()} into this.
     *
     * @param serialized A sketch serialized using the serialize method.
     */
    public abstract void union(byte[] serialized);

    /**
     * Collects the data presented to the sketch and returns it as a {@link Clip}. Also adds {@link Metadata} if
     * asked for.
     *
     * @param metaKey If set to a non-null value, Sketch metadata will be added to the result.
     * @param conceptKeys If provided, these {@link Concept} will be added to the metadata.
     *
     * @return A {@link Clip} of the results.
     */
    public Clip getResult(String metaKey, Map<String, String> conceptKeys) {
        // Subclasses are charge of adding data
        collect();
        return metaKey == null ? new Clip() : Clip.of(new Metadata().add(metaKey, getMetadata(conceptKeys)));
    }

    /**
     * Gets the common metadata for this Sketch.
     *
     * @param conceptKeys The {@link Map} of {@link Concept} names to their keys.
     * @return The created {@link Map} of sketch metadata.
     */
    protected Map<String, Object> getMetadata(Map<String, String> conceptKeys) {
        Map<String, Object> metadata = new HashMap<>();
        addIfNonNull(metadata, conceptKeys.get(Concept.FAMILY.getName()), this::getFamily);
        addIfNonNull(metadata, conceptKeys.get(Concept.SIZE.getName()), this::getSize);
        addIfNonNull(metadata, conceptKeys.get(Concept.ESTIMATED_RESULT.getName()), this::isEstimationMode);
        return metadata;
    }

    /**
     * Resets the Sketch to the original state. The old results are lost. You should call this after you call
     * {@link #serialize()} or {@link #getResult(String, Map)} if you want to add more data to the sketch.
     */
    public abstract void reset();

    /**
     * Collects the data presented to the Sketch so far.
     */
    protected abstract void collect();


    /**
     * Returns a String representing the family of this sketch.
     *
     * @return The String family of this sketch.
     */
    protected abstract String getFamily();

    /**
     * Returns whether this sketch was in estimation mode or not after the last collect. Only applicable after {@link #collect()}.
     *
     * @return A Boolean denoting whether this sketch was estimating.
     * @throws NullPointerException if collect had not been called.
     */
    protected abstract Boolean isEstimationMode();

    /**
     * Returns the size of the Sketch in bytes. Only applicable after {@link #collect()}.
     *
     * @return An Integer representing the size of the sketch.
     * @throws NullPointerException if collect had not been called.
     */
    protected abstract Integer getSize();

    /**
     * Utility function to add a key to the metadata if the key and value are not null.
     *
     * @param metadata The non-null {@link Map} representing the metadata.
     * @param key The key to add if not null.
     * @param supplier A {@link Supplier} that can produce a value to add to the metadata for the key. If the supplier
     *                 produces null, it is not added.
     */
    static void addIfNonNull(Map<String, Object> metadata, String key, Supplier<Object> supplier) {
        Object data = null;
        if (key != null) {
            data = supplier.get();
        }
        if (data != null) {
            metadata.put(key, data);
        }
    }
}
