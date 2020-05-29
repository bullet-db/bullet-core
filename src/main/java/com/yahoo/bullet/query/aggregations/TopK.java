/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.querying.aggregations.FrequentItemsSketchingStrategy;
import com.yahoo.bullet.querying.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Getter
public class TopK extends Aggregation {
    private static final long serialVersionUID = 2934123789816778466L;
    private static final BulletException TOP_K_REQUIRES_FIELDS =
            new BulletException("TOP K requires at least one field.", "Please add at least one field.");

    private final Map<String, String> fieldsToNames;
    private final Long threshold;
    private final String name;

    /**
     * Constructor that creates a TOP_K aggregation with a specified max size.
     *
     * @param fieldsToNames The mapping of fields to aliases of the Top K aggregation. Must not be empty.
     * @param size The max size of the Top K aggregation. Can be null.
     * @param threshold The minimum threshold of the Top K aggregation. Can be null.
     * @param name The name of the count field.
     */
    public TopK(Map<String, String> fieldsToNames, Integer size, Long threshold, String name) {
        super(size, AggregationType.TOP_K);
        Utilities.requireNonNull(fieldsToNames);
        if (fieldsToNames.isEmpty()) {
            throw TOP_K_REQUIRES_FIELDS;
        }
        this.fieldsToNames = fieldsToNames;
        this.threshold = threshold;
        this.name = Objects.requireNonNull(name);
    }

    @Override
    public Strategy getStrategy(BulletConfig config) {
        return new FrequentItemsSketchingStrategy(this, config);
    }

    @Override
    public List<String> getFields() {
        return new ArrayList<>(fieldsToNames.keySet());
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", fieldsToNames: " + fieldsToNames + ", threshold: " + threshold + ", name: " + getName() + "}";
    }
}
