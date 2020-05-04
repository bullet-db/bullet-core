/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.aggregations.CountDistinct;
import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;
import lombok.Getter;

import java.util.List;
import java.util.Objects;

@Getter
public class CountDistinctAggregation extends Aggregation {
    private static final long serialVersionUID = 3079494553075374672L;

    private List<String> fields;
    private String name;

    /**
     * Constructor that creates a COUNT_DISTINCT aggregation.
     *
     * @param fields List of fields to count distinct on.
     * @param name Name of count distinct field.
     */
    public CountDistinctAggregation(List<String> fields, String name) {
        super(null, Type.COUNT_DISTINCT);
        Utilities.requireNonNullList(fields);
        if (fields.isEmpty()) {
            throw new BulletException("COUNT DISTINCT requires at least one field.", "Please add at least one field.");
        }
        this.fields = fields;
        this.name = Objects.requireNonNull(name);
    }

    @Override
    public Strategy getStrategy(BulletConfig config) {
        return new CountDistinct(this, config);
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", fields: " + fields + ", name: " + name + "}";
    }
}
