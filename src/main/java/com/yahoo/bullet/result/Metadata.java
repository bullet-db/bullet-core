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

import static java.util.Arrays.asList;

public class Metadata {
    private Map<String, Object> meta = new HashMap<>();
    public static final List<Concept> KNOWN_CONCEPTS = asList(Concept.values());

    @Getter
    public enum Concept {
        QUERY_RECEIVE_TIME("Query Receive Time"),
        RESULT_EMIT_TIME("Result Emit Time"),
        QUERY_FINISH_TIME("Query Finish Time"),
        QUERY_ID("Query Identifier"),
        QUERY_BODY("Query Body"),

        // Sketching metadata
        SKETCH_METADATA("Sketch Metadata"),
        ESTIMATED_RESULT("Estimated Result"),
        UNIQUES_ESTIMATE("Uniques Estimate"),
        STANDARD_DEVIATIONS("Standard Deviations"),
        FAMILY("Family"),
        SIZE("Size"),
        THETA("Theta"),
        MINIMUM_VALUE("Minimum Value"),
        MAXIMUM_VALUE("Maximum Value"),
        ITEMS_SEEN("Items Seen"),
        NORMALIZED_RANK_ERROR("Normalized Rank Error"),
        MAXIMUM_COUNT_ERROR("Maximum Count Error"),
        ACTIVE_ITEMS("Active Items"),

        // Windowing metadata
        WINDOW_METADATA("Window Metadata"),
        WINDOW_NAME("Window Name"),
        WINDOW_COUNT("Window Count");

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
    public Metadata add(String key, Object information) {
        meta.put(key, information);
        return this;
    }

    /**
     * Add errors to the Metadata.
     *
     * @param errors {@link BulletError} objects to add.
     * @return This object for chaining.
     */
    @SuppressWarnings("unchecked")
    public Metadata addErrors(List<BulletError> errors) {
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
     * Static construction of Metadata with some errors.
     *
     * @param errors A non-null {@link List} of {@link BulletError} objects.
     * @return The Metadata object with the errors.
     */
    public static Metadata of(BulletError... errors) {
        Metadata meta = new Metadata();
        meta.addErrors(asList(errors));
        return meta;
    }

    /**
     * Static construction of Metadata with some errors.
     *
     * @param errors A non-null list of {@link BulletError} objects.
     * @return The Metadata object with the errors.
     */
    public static Metadata of(List<BulletError> errors) {
        Metadata meta = new Metadata();
        meta.addErrors(errors);
        return meta;
    }

    /**
     * Merge another Metadata into this Metadata.
     *
     * @param metadata A Metadata to merge.
     * @return This Object after the merge.
     */
    public Metadata merge(Metadata metadata) {
        if (metadata != null) {
            this.meta.putAll(metadata.asMap());
        }
        return this;
    }
}
