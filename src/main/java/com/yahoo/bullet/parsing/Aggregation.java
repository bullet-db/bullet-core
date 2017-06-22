/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.BulletConfig;
import com.yahoo.bullet.Utilities;
import com.yahoo.bullet.operations.AggregationOperations.AggregationType;
import com.yahoo.bullet.operations.aggregations.CountDistinct;
import com.yahoo.bullet.operations.aggregations.Distribution;
import com.yahoo.bullet.operations.aggregations.GroupAll;
import com.yahoo.bullet.operations.aggregations.GroupBy;
import com.yahoo.bullet.operations.aggregations.Raw;
import com.yahoo.bullet.operations.aggregations.Strategy;
import com.yahoo.bullet.operations.aggregations.TopK;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.yahoo.bullet.parsing.Error.makeError;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@Getter @Setter
public class Aggregation implements Configurable, Validatable {
    @Expose
    private Integer size;
    @Expose
    private AggregationType type;
    @Expose
    private Map<String, Object> attributes;
    @Expose
    private Map<String, String> fields;

    @Setter(AccessLevel.NONE)
    private Strategy strategy;

    // In case any strategies need it.
    private Map configuration;

    public static final Set<AggregationType> SUPPORTED_AGGREGATION_TYPES =
            new HashSet<>(asList(AggregationType.GROUP, AggregationType.COUNT_DISTINCT, AggregationType.RAW,
                                 AggregationType.DISTRIBUTION, AggregationType.TOP_K));
    public static final Error TYPE_NOT_SUPPORTED_ERROR =
            makeError("Unknown aggregation type", "Current supported aggregation types are: RAW (or LIMIT), " +
                                                  "GROUP (or DISTINCT), COUNT DISTINCT, DISTRIBUTION, TOP K");

    public static final Integer DEFAULT_SIZE = 1;
    public static final Integer DEFAULT_MAX_SIZE = 512;

    public static final String DEFAULT_FIELD_SEPARATOR = "|";


    /**
     * Default constructor. GSON recommended
     */
    public Aggregation() {
        type = AggregationType.RAW;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map configuration) {
        this.configuration = configuration;

        Number defaultSize = (Number) configuration.getOrDefault(BulletConfig.AGGREGATION_DEFAULT_SIZE, DEFAULT_SIZE);
        Number maximumSize = (Number) configuration.getOrDefault(BulletConfig.AGGREGATION_MAX_SIZE, DEFAULT_MAX_SIZE);
        int sizeDefault = defaultSize.intValue();
        int sizeMaximum = maximumSize.intValue();

        // Null or negative, then default, else min of size and max
        size = (size == null || size < 0) ? sizeDefault : Math.min(size, sizeMaximum);

        strategy = findStrategy();
    }

    @Override
    public Optional<List<Error>> validate() {
        if (strategy == null) {
            return Optional.of(singletonList(TYPE_NOT_SUPPORTED_ERROR));
        }
        List<Error> errors = strategy.initialize();
        return Utilities.isEmpty(errors) ? Optional.empty() : Optional.of(errors);
    }

    /**
     * Returns a new {@link Strategy} instance that can handle this aggregation.
     *
     * @return the created instance of a strategy that can implement the provided AggregationType or null if it cannot.
     */
    Strategy findStrategy() {
        // Includes null
        if (!SUPPORTED_AGGREGATION_TYPES.contains(type)) {
            return null;
        }
        switch (type) {
            case COUNT_DISTINCT:
                return new CountDistinct(this);
            case DISTRIBUTION:
                return new Distribution(this);
            case RAW:
                return new Raw(this);
            case TOP_K:
                return new TopK(this);
        }
        // If we have any fields -> GroupBy
        return Utilities.isEmpty(fields) ? new GroupAll(this) : new GroupBy(this);
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", fields: " + fields + ", attributes: " + attributes + "}";
    }

}
