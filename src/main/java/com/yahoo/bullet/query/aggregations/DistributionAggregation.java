/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.aggregations.Distribution;
import com.yahoo.bullet.aggregations.Strategy;
import com.yahoo.bullet.aggregations.sketches.QuantileSketch;
import com.yahoo.bullet.common.BulletConfig;
import lombok.Getter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Getter
public abstract class DistributionAggregation extends Aggregation {
    private static final long serialVersionUID = -7862051610403543796L;

    protected final String field;
    protected final Distribution.Type distributionType;

    /**
     * Constructor that creates a DISTRIBUTION aggregation with a specified max size.
     *
     * @param field The non-null field
     * @param type The non-null distribution type
     * @param size The max size of the DISTRIBUTION aggregation. Can be null.
     */
    protected DistributionAggregation(String field, Distribution.Type type, Integer size) {
        super(size, Type.DISTRIBUTION);
        this.field = Objects.requireNonNull(field, "The field must be non-null.");
        this.distributionType = Objects.requireNonNull(type, "The distribution type must be non-null.");
    }

    @Override
    public Strategy getStrategy(BulletConfig config) {
        return new Distribution(this, config);
    }

    @Override
    public List<String> getFields() {
        return Collections.singletonList(field);
    }

    public abstract QuantileSketch getSketch(BulletConfig config);
}
