/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.common.Initializable;
import com.yahoo.bullet.querying.AggregationOperations.AggregationType;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.yahoo.bullet.parsing.ParsingError.makeError;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@Getter @Setter
public class Aggregation implements Serializable, Configurable, Initializable {
    @Expose
    private Integer size;
    @Expose
    private AggregationType type;
    @Expose
    private Map<String, Object> attributes;
    @Expose
    private Map<String, String> fields;

    public static final Set<AggregationType> SUPPORTED_AGGREGATION_TYPES =
            new HashSet<>(asList(AggregationType.GROUP, AggregationType.COUNT_DISTINCT, AggregationType.RAW,
                                 AggregationType.DISTRIBUTION, AggregationType.TOP_K));
    public static final ParsingError TYPE_NOT_SUPPORTED_ERROR =
            makeError("Unknown aggregation type", "Current supported aggregation types are: RAW (or LIMIT), " +
                                                  "GROUP (or DISTINCT), COUNT DISTINCT, DISTRIBUTION, TOP K");

    /**
     * Default constructor. GSON recommended
     */
    public Aggregation() {
        type = AggregationType.RAW;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(BulletConfig config) {
        int sizeDefault = config.getAs(BulletConfig.AGGREGATION_DEFAULT_SIZE, Integer.class);
        int sizeMaximum = config.getAs(BulletConfig.AGGREGATION_MAX_SIZE, Integer.class);

        // Null or negative, then default, else min of size and max
        size = (size == null || size < 0) ? sizeDefault : Math.min(size, sizeMaximum);
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        // Includes null
        return SUPPORTED_AGGREGATION_TYPES.contains(type) ? Optional.empty() :
                                                            Optional.of(singletonList(TYPE_NOT_SUPPORTED_ERROR));
    }

    @Override
    public String toString() {
        return "{size: " + size + ", type: " + type + ", fields: " + fields + ", attributes: " + attributes + "}";
    }
}
