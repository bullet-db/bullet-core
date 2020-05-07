/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.aggregations;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Configurable;
import lombok.Getter;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Getter
public class Aggregation implements Configurable, Serializable {
    private static final long serialVersionUID = -4451469769203362270L;

    /** Represents the type of the Aggregation. */
    public enum Type {
        GROUP,
        COUNT_DISTINCT,
        TOP_K,
        DISTRIBUTION,
        RAW
    }

    protected Integer size;
    protected Type type;

    /**
     * Default constructor that creates a RAW aggregation with no specified size.
     */
    public Aggregation() {
        size = null;
        type = Type.RAW;
    }

    /**
     * Constructor that creates a RAW aggregation with a specified max size.
     *
     * @param size The max size of the RAW aggregation. Can be null.
     */
    public Aggregation(Integer size) {
        this.size = size;
        this.type = Type.RAW;
    }

    protected Aggregation(Integer size, Type type) {
        this.size = size;
        this.type = Objects.requireNonNull(type);
    }

    @Override
    public void configure(BulletConfig config) {
        int sizeDefault = config.getAs(BulletConfig.AGGREGATION_DEFAULT_SIZE, Integer.class);
        int sizeMaximum = config.getAs(BulletConfig.AGGREGATION_MAX_SIZE, Integer.class);

        // Null or not positive, then default, else min of size and max
        size = (size == null || size <= 0) ? sizeDefault : Math.min(size, sizeMaximum);
    }

    /**
     * Returns a new {@link Strategy} instance that can handle this aggregation.
     *
     * @param config The {@link BulletConfig} containing configuration for the strategy.
     *
     * @return The created instance of a strategy that can implement this aggregation.
     */
    public Strategy getStrategy(BulletConfig config) {
        return new Raw(this, config);
    }

    public List<String> getFields() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + "}";
    }
}
