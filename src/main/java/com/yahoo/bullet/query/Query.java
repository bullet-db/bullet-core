/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.query.aggregations.Aggregation;
import com.yahoo.bullet.query.aggregations.AggregationType;
import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.postaggregations.PostAggregation;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * This class is the top level Bullet Query specification. It holds the definition of the Query.
 */
@Getter @Slf4j
public class Query implements Configurable, Serializable {
    private static final long serialVersionUID = 592082288228551406L;

    private final Projection projection;
    private final Expression filter;
    private final Aggregation aggregation;
    private final List<PostAggregation> postAggregations;
    private Window window;
    private Long duration;

    public static final BulletException ONLY_RAW_RECORD = new BulletException("Only \"RAW\" aggregation types can have window emit type \"RECORD\"",
                                                                              "Change your aggregation type or your window emit type to \"TIME\"");
    public static final BulletException NO_RAW_ALL = new BulletException("The \"RAW\" aggregation type cannot have window include \"ALL\"",
                                                                         "Change your aggregation type or your window include type");

    public Query(Projection projection, Expression filter, Aggregation aggregation, List<PostAggregation> postAggregations, Window window, Long duration) {
        this.projection = Objects.requireNonNull(projection);
        this.filter = filter;
        this.aggregation = Objects.requireNonNull(aggregation);
        this.postAggregations = postAggregations;
        this.window = Objects.requireNonNull(window);
        this.duration = duration;
        // Required since there are window types that are not yet supported.
        validateWindow();
    }

    private void validateWindow() {
        AggregationType type = aggregation.getType();
        Window.Classification kind = window.getType();
        if (type != AggregationType.RAW && kind == Window.Classification.RECORD_RECORD) {
            throw ONLY_RAW_RECORD;
        }
        if (type == AggregationType.RAW && kind == Window.Classification.TIME_ALL) {
            throw NO_RAW_ALL;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(BulletConfig config) {
        aggregation.configure(config);

        boolean disableWindowing = config.getAs(BulletConfig.WINDOW_DISABLE, Boolean.class);
        if (disableWindowing) {
            window = new Window();
        }
        window.configure(config);

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
