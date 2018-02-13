/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.result;

import com.yahoo.bullet.common.BulletError;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Arrays.asList;

public class Meta {
    private Map<String, Object> meta = new HashMap<>();
    public static final List<Concept> KNOWN_CONCEPTS = asList(Concept.values());

    @Getter
    public enum Concept {
        QUERY_RECEIVE_TIME("Query Receive Time"),
        RESULT_EMIT_TIME("Result Emit Time"),
        QUERY_FINISH_TIME("Query Finish Time"),
        QUERY_ID("Query ID"),
        QUERY_BODY("Query Body"),

        // Sketching metadata
        SKETCH_METADATA("Sketch Metadata"),
        SKETCH_ESTIMATED_RESULT("Sketch Estimated Result"),
        SKETCH_UNIQUES_ESTIMATE("Sketch Uniques Estimate"),
        SKETCH_STANDARD_DEVIATIONS("Sketch Standard Deviations"),
        SKETCH_FAMILY("Sketch Family"),
        SKETCH_SIZE("Sketch Size"),
        SKETCH_THETA("Sketch Theta"),
        SKETCH_MINIMUM_VALUE("Sketch Minimum Value"),
        SKETCH_MAXIMUM_VALUE("Sketch Maximum Value"),
        SKETCH_ITEMS_SEEN("Sketch Items Seen"),
        SKETCH_NORMALIZED_RANK_ERROR("Sketch Normalized Rank Error"),
        SKETCH_MAXIMUM_COUNT_ERROR("Sketch Maximum Count Error"),
        SKETCH_ACTIVE_ITEMS("Sketch Active Items"),

        // Windowing metadata
        WINDOW_METADATA("Window Metadata"),
        WINDOW_NAME("Window Name"),
        WINDOW_NUMBER("Window Number"),
        WINDOW_SIZE("Window Size"),
        WINDOW_EMIT_TIME("Window Emit Time"),
        WINDOW_EXPECTED_EMIT_TIME("Window Expected Emit Time");

        private String name;

        Concept(String name) {
            this.name = name;
        }

        /**
         * Returns true iff the given String concept is this Concept.
         *
         * @param concept The String version of this concept.
         * @return A boolean denoting whether this concept is this String.
         */
        public boolean isMe(String concept) {
            return name.equals(concept);
        }

        /**
         * Creates a Concept instance from a String version of it.
         *
         * @param concept The string version of the Concept.
         * @return A Concept or null if the string does not match any known Concept.
         */
        public static Concept from(String concept) {
            return KNOWN_CONCEPTS.stream().filter(c -> c.isMe(concept)).findFirst().orElse(null);
        }
    }

    // This is not a Concept because it is not configurable. It will be returned no matter what with this key.
    public static final String ERROR_KEY = "errors";

    /**
     * Returns a backing view of the meta information as a Map.
     *
     * @return A Map of keys to objects that denote the meta information.
     */
    public Map<String, Object> asMap() {
        return meta;
    }

    /**
     * Add a piece of meta information.
     *
     * @param key The name of the meta tag
     * @param information An object that represents the information.
     * @return This object for chaining.
     */
    public Meta add(String key, Object information) {
        meta.put(key, information);
        return this;
    }

    /**
     * Add errors to the Meta.
     *
     * @param errors {@link BulletError} objects to add.
     * @return This object for chaining.
     */
    @SuppressWarnings("unchecked")
    public Meta addErrors(List<BulletError> errors) {
        Objects.requireNonNull(errors);
        List<BulletError> existing = (List<BulletError>) meta.get(ERROR_KEY);
        if (existing != null) {
            existing.addAll(errors);
        } else {
            meta.put(ERROR_KEY, new ArrayList<>(errors));
        }
        return this;
    }

    /**
     * Static construction of Meta with some errors.
     *
     * @param errors A non-null {@link List} of {@link BulletError} objects.
     * @return The Meta object with the errors.
     */
    public static Meta of(BulletError... errors) {
        Meta meta = new Meta();
        meta.addErrors(asList(errors));
        return meta;
    }

    /**
     * Static construction of Meta with some errors.
     *
     * @param errors A non-null list of {@link BulletError} objects.
     * @return The Meta object with the errors.
     */
    public static Meta of(List<BulletError> errors) {
        Meta meta = new Meta();
        meta.addErrors(errors);
        return meta;
    }

    /**
     * Merge another Meta into this Meta.
     *
     * @param meta A Meta to merge.
     * @return This Object after the merge.
     */
    public Meta merge(Meta meta) {
        if (meta != null) {
            this.meta.putAll(meta.asMap());
        }
        return this;
    }

    /**
     * Utility function to add a concept with a configured name to a map representing metadata if both the name and
     * and a value for the name produced by a given {@link Supplier} are not null.
     *
     * @param meta The non-null {@link Map} representing the metadata.
     * @param names The non-null {@link Map} of configured {@link Concept} names to key names to use.
     * @param concept The concept to add
     * @param supplier A {@link Supplier} that can produce a value to add to the metadata for the concept. If the
     *                 supplier produces null, it is not added.
     */
    public static void addIfNonNull(Map<String, Object> meta, Map<String, String> names, Concept concept,
                                    Supplier<Object> supplier) {
        Object data = null;
        String key = names.get(concept.getName());
        if (key != null) {
            data = supplier.get();
        }
        if (data != null) {
            meta.put(key, data);
        }
    }

    /**
     * Utility function to apply a method for a given {@link Concept} name if provided in the given {@link Map} of names.
     *
     * @param concept The concept to check in the map if present.
     * @param names A non-null map of concept names to their key names.
     * @param action The action to apply if the concept was provided in the map.
     */
    public static void consumeRegisteredConcept(Concept concept, Map<String, String> names, Consumer<String> action) {
        // Only consume the concept if we have a key for it: i.e. it was registered
        String key = names.get(concept.getName());
        if (key != null) {
            action.accept(key);
        }
    }
}
