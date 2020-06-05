/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.querying.aggregations.ThetaSketchingStrategy;
import com.yahoo.bullet.querying.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;
import lombok.Getter;

import java.util.List;
import java.util.Objects;

@Getter
public class CountDistinct extends Aggregation {
    private static final long serialVersionUID = 3079494553075374672L;
    private static final BulletException COUNT_DISTINCT_REQUIRES_FIELDS =
            new BulletException("COUNT DISTINCT requires at least one field.", "Please add at least one field.");

    private final List<String> fields;
    private final String name;

    /**
     * Constructor that creates a COUNT_DISTINCT aggregation.
     *
     * @param fields The list of fields to count distinct on.
     * @param name The name of the count distinct field.
     */
    public CountDistinct(List<String> fields, String name) {
        super(null, AggregationType.COUNT_DISTINCT);
        Utilities.requireNonNull(fields);
        if (fields.isEmpty()) {
            throw COUNT_DISTINCT_REQUIRES_FIELDS;
        }
        this.fields = fields;
        this.name = Objects.requireNonNull(name);
    }

    @Override
    public Strategy getStrategy(BulletConfig config) {
        return new ThetaSketchingStrategy(this, config);
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", fields: " + fields + ", name: " + name + "}";
    }
}
