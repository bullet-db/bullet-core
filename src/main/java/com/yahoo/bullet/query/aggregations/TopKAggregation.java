/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.aggregations.TopK;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TopKAggregation extends Aggregation {
    private static final long serialVersionUID = 2934123789816778466L;

    public static final String DEFAULT_NAME = "COUNT";

    private Map<String, String> fields;
    @Getter @Setter
    private Long threshold;
    @Setter
    private String name;

    /**
     * Constructor that creates a TOP_K aggregation with a specified max size.
     *
     * @param fields The fields of the Top K aggregation. Must not be empty.
     * @param size The max size of the TOP_K aggregation. Can be null.
     */
    public TopKAggregation(Map<String, String> fields, Integer size) {
        super(size, Type.TOP_K);
        Utilities.requireNonNullMap(fields);
        if (fields.isEmpty()) {
            throw new BulletException("TOP K requires at least one field.", "Please add at least one field.");
        }
        this.fields = fields;
    }

    @Override
    public Strategy getStrategy(BulletConfig config) {
        return new TopK(this, config);
    }

    @Override
    public List<String> getFields() {
        return new ArrayList<>(fields.keySet());
    }

    public Map<String, String> getFieldsToNames() {
        return fields;
    }

    public String getName() {
        return name != null ? name : DEFAULT_NAME;
    }
}
