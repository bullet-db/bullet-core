/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.query.postaggregations.PostAggregation;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import static com.yahoo.bullet.common.BulletError.makeError;

/**
 * This class is the top level Bullet Query specification. It holds the definition of the Query.
 */
@Getter @Slf4j
public class Query implements Configurable, Serializable {
    private static final long serialVersionUID = 592082288228551406L;

    private Projection projection;
    private Expression filter;
    private Aggregation aggregation;
    private List<PostAggregation> postAggregations;
    private Window window;
    private Long duration;

    public static final BulletError IMMUTABLE_RECORD = makeError("Cannot have computation/culling post aggregation with \"RAW\" aggregation type and pass-through projection",
                                                                 "This is a bug if this query came from BQL");
    public static final BulletError ONLY_RAW_RECORD = makeError("Only \"RAW\" aggregation types can have window emit type \"RECORD\"",
                                                                "Change your aggregation type or your window emit type to \"TIME\"");
    public static final BulletError NO_RAW_ALL = makeError("The \"RAW\" aggregation types cannot have window include \"ALL\"",
                                                           "Change your aggregation type or your window include type");

    public Query(Projection projection, Expression filter, Aggregation aggregation, List<PostAggregation> postAggregations, Window window, Long duration) {
        this.projection = Objects.requireNonNull(projection);
        this.filter = filter;
        this.aggregation = Objects.requireNonNull(aggregation);
        this.postAggregations = postAggregations;
        this.window = Objects.requireNonNull(window);
        this.duration = duration;
        validateWindow();
        validatePostAggregations();
    }

    private void validateWindow() {
        Aggregation.Type type = aggregation.getType();
        Window.Classification kind = window.getType();
        if (type != Aggregation.Type.RAW && kind == Window.Classification.RECORD_RECORD) {
            throw new IllegalArgumentException("only RAW aggregation can have window emit type RECORD");
        }
        if (type == Aggregation.Type.RAW && kind == Window.Classification.TIME_ALL) {
            throw new IllegalArgumentException("RAW aggregation can't have window include ALL");
        }
    }

    private void validatePostAggregations() {
        if (postAggregations == null || postAggregations.isEmpty()) {
            return;
        }
        // make sure postaggregations are distinct and in order
        int prev = -1;
        for (PostAggregation postAggregation : postAggregations) {
            int priority = postAggregation.getType().getPriority();
            if (priority < prev) {
                throw new IllegalArgumentException("order of post aggregations is incorrect");
            } else if (priority == prev) {
                throw new IllegalArgumentException("cannot have more than one of a postaggregation");
            } else {
                prev = priority;
            }
        }
        // immutable record
        if (projection.getType() == Projection.Type.PASS_THROUGH && aggregation.getType() == Aggregation.Type.RAW &&
            postAggregations.stream().anyMatch(postAggregation -> postAggregation instanceof Computation ||
                                                                  postAggregation instanceof Culling)) {
            throw new IllegalArgumentException("Cannot have computation/culling post aggregation with RAW aggregation type and pass-through projection");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(BulletConfig config) {
        aggregation.configure(config);

        boolean disableWindowing = config.getAs(BulletConfig.WINDOW_DISABLE, Boolean.class);
        if (disableWindowing) {
            window = new Window();
        } else if (window != null) {
            window.configure(config);
        }

        long durationDefault = config.getAs(BulletConfig.QUERY_DEFAULT_DURATION, Long.class);
        long durationMax = config.getAs(BulletConfig.QUERY_MAX_DURATION, Long.class);

        // Null or negative, then default, else min of duration and max.
        duration = (duration == null || duration <= 0) ? durationDefault : Math.min(duration, durationMax);
    }

    @Override
    public String toString() {
        return "{filter: " + filter + ", projection: " + projection + ", aggregation: " + aggregation +
                ", postAggregations: " + postAggregations + ", window: " + window + ", duration: " + duration + "}";
    }
}
