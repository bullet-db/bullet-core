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
import com.yahoo.bullet.parsing.expressions.Expression;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.yahoo.bullet.common.BulletError.makeError;

/**
 * This class is the top level Bullet Query specification. It holds the definition of the Query.
 */
@Getter @Setter @Slf4j
public class Query implements Configurable, Initializable {
    @Expose
    private Projection projection;
    @Expose
    private Expression filter;
    @Expose
    private Aggregation aggregation;
    @Expose
    private Window window;
    @Expose
    private Long duration;
    @Expose
    private List<PostAggregation> postAggregations;

    public static final BulletError ONLY_RAW_RECORD = makeError("Only \"RAW\" aggregation types can have window emit type \"RECORD\"",
                                                                "Change your aggregation type or your window emit type to \"TIME\"");
    public static final BulletError NO_RAW_ALL = makeError("The \"RAW\" aggregation types cannot have window include \"ALL\"",
                                                           "Change your aggregation type or your window include type");
    public static final BulletError NO_DUPLICATE_POST_AGGREGATIONS = makeError("The post aggregations cannot have multiple of the same type.",
                                                                               "Change your post aggregations to keep at most one of each.");

    /**
     * Default constructor. GSON recommended.
     */
    public Query() {
        // If no aggregation is provided, the default one is used. An aggregation must be present.
        aggregation = new Aggregation();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(BulletConfig config) {
        if (projection != null) {
            projection.configure(config);
        }
        if (filter != null) {
            filter.configure(config);
        }
        // Must have an aggregation
        if (aggregation == null) {
            aggregation = new Aggregation();
        }
        aggregation.configure(config);

        boolean disableWindowing = config.getAs(BulletConfig.WINDOW_DISABLE, Boolean.class);
        if (disableWindowing) {
            window = null;
        } else if (window != null) {
            window.configure(config);
        }

        long durationDefault = config.getAs(BulletConfig.QUERY_DEFAULT_DURATION, Long.class);
        long durationMax = config.getAs(BulletConfig.QUERY_MAX_DURATION, Long.class);

        // Null or negative, then default, else min of duration and max.
        duration = (duration == null || duration <= 0) ? durationDefault : Math.min(duration, durationMax);

        if (postAggregations != null) {
            postAggregations.forEach(p -> p.configure(config));
        }
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        List<BulletError> errors = new ArrayList<>();
        if (projection != null) {
            projection.initialize().ifPresent(errors::addAll);
        }
        if (filter != null) {
            filter.initialize().ifPresent(errors::addAll);
        }

        aggregation.initialize().ifPresent(errors::addAll);

        if (postAggregations != null) {
            if (postAggregations.stream().map(PostAggregation::getType).distinct().count() < postAggregations.size()) {
                errors.add(NO_DUPLICATE_POST_AGGREGATIONS);
            }
            postAggregations.forEach(p -> p.initialize().ifPresent(errors::addAll));
        }
        if (window != null) {
            window.initialize().ifPresent(errors::addAll);
            Aggregation.Type type = aggregation.getType();
            Window.Classification kind = window.getType();
            if (type != Aggregation.Type.RAW && kind == Window.Classification.RECORD_RECORD) {
                errors.add(ONLY_RAW_RECORD);
            }
            if (type == Aggregation.Type.RAW && kind == Window.Classification.TIME_ALL) {
                errors.add(NO_RAW_ALL);
            }
        }
        return errors.isEmpty() ? Optional.empty() : Optional.of(errors);
    }


    @Override
    public String toString() {
        return "{filter: " + filter + ", projection: " + projection + ", aggregation: " + aggregation +
                ", postAggregations: " + postAggregations + ", window: " + window + ", duration: " + duration + "}";
    }
}
