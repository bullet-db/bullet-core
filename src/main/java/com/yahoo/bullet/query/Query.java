/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.common.Initializable;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.query.postaggregations.PostAggregation;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

/**
 * This class is the top level Bullet Query specification. It holds the definition of the Query.
 */
@Getter @Setter @Slf4j
public class Query implements Configurable, Initializable {
    private Projection projection;
    private Expression filter;
    private Aggregation aggregation;
    private Window window;
    private Long duration;
    private List<PostAggregation> postAggregations;

    public static final BulletError IMMUTABLE_RECORD = makeError("Cannot have computation/culling post aggregation with \"RAW\" aggregation type and pass-through projection",
                                                                 "This is a bug if this query came from BQL");
    public static final BulletError ONLY_RAW_RECORD = makeError("Only \"RAW\" aggregation types can have window emit type \"RECORD\"",
                                                                "Change your aggregation type or your window emit type to \"TIME\"");
    public static final BulletError NO_RAW_ALL = makeError("The \"RAW\" aggregation types cannot have window include \"ALL\"",
                                                           "Change your aggregation type or your window include type");

    @Override
    @SuppressWarnings("unchecked")
    public void configure(BulletConfig config) {
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
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        List<BulletError> errors = new ArrayList<>();
        if (postAggregations != null) {
            if (projection.getType() == Projection.Type.PASS_THROUGH &&
                aggregation.getType() == Aggregation.Type.RAW &&
                postAggregations.stream().anyMatch(postAggregation -> postAggregation instanceof Computation ||
                                                                      postAggregation instanceof Culling)) {
                errors.add(IMMUTABLE_RECORD);
            }
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
