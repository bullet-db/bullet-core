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
import com.yahoo.bullet.query.tablefunctions.TableFunction;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * This class is the top level Bullet query specification. It holds the definition of the query.
 */
@Getter @Slf4j
public class Query implements Configurable, Serializable {
    private static final long serialVersionUID = 592082288228551406L;

    private final TableFunction tableFunction;
    private final Projection projection;
    private final Expression filter;
    private final Aggregation aggregation;
    private final List<PostAggregation> postAggregations;
    private final Query outerQuery;
    private Window window;
    private Long duration;

    private static final BulletException ONLY_RAW_RECORD = new BulletException("Only RAW aggregation type can have window emit type RECORD.",
                                                                               "Change your aggregation type or your window emit type to TIME.");
    private static final BulletException NO_RAW_ALL = new BulletException("RAW aggregation type cannot have window include type ALL.",
                                                                          "Change your aggregation type or your window include type.");
    private static final BulletException NO_OUTER_QUERY_WINDOW = new BulletException("Outer query cannot have a window.",
                                                                                     "Remove the window.");
    private static final BulletException NO_NESTED_OUTER_QUERY = new BulletException("Outer query cannot have an outer query.",
                                                                                     "Remove the nested outer query.");

    /**
     * Constructor that creates the Bullet query.
     *
     * @param tableFunction The table function that is applied to the input record before the rest of the query. Can be null.
     * @param projection The non-null projection that decides which fields are selected from a Bullet record before aggregation.
     * @param filter The filter expression records must pass before projection. Can be null.
     * @param aggregation The non-null aggregation that takes projected records.
     * @param postAggregations The list of post-aggregations that are executed on records before getting results. Can be null.
     * @param outerQuery The outer query that is executed on records before getting results. Can be null.
     * @param window The non-null window that decides when and how results are returned.
     * @param duration The duration of the query. Can be null.
     */
    public Query(TableFunction tableFunction, Projection projection, Expression filter, Aggregation aggregation, List<PostAggregation> postAggregations, Query outerQuery, Window window, Long duration) {
        this.tableFunction = tableFunction;
        this.projection = Objects.requireNonNull(projection);
        this.filter = filter;
        this.aggregation = Objects.requireNonNull(aggregation);
        this.postAggregations = postAggregations;
        this.outerQuery = outerQuery;
        this.window = Objects.requireNonNull(window);
        this.duration = duration;
        // Required since there are window types that are not yet supported.
        validateWindow();
        validateOuterQuery();
    }

    /**
     * Constructor that creates the Bullet query.
     *
     * @param tableFunction The table function that is applied to the input record before the rest of the query. Can be null.
     * @param projection The non-null projection that decides which fields are selected from a Bullet record before aggregation.
     * @param filter The filter expression records must pass before projection. Can be null.
     * @param aggregation The non-null aggregation that takes projected records.
     * @param postAggregations The list of post-aggregations that are executed on records before getting results. Can be null.
     * @param window The non-null window that decides when and how results are returned.
     * @param duration The duration of the query. Can be null.
     */
    public Query(TableFunction tableFunction, Projection projection, Expression filter, Aggregation aggregation, List<PostAggregation> postAggregations, Window window, Long duration) {
        this(tableFunction, projection, filter, aggregation, postAggregations, null, window, duration);
    }

    /**
     * Constructor that creates the Bullet query.
     *
     * @param projection The non-null projection that decides which fields are selected from a Bullet record before aggregation.
     * @param filter The filter expression records must pass before projection. Can be null.
     * @param aggregation The non-null aggregation that takes projected records.
     * @param postAggregations The list of post-aggregations that are executed on records before getting results. Can be null.
     * @param window The non-null window that decides when and how results are returned.
     * @param duration The duration of the query. Can be null.
     */
    public Query(Projection projection, Expression filter, Aggregation aggregation, List<PostAggregation> postAggregations, Window window, Long duration) {
        this(null, projection, filter, aggregation, postAggregations, window, duration);
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

    private void validateOuterQuery() {
        if (outerQuery == null) {
            return;
        }
        if (outerQuery.getWindow().getType() != null) {
            throw NO_OUTER_QUERY_WINDOW;
        }
        if (outerQuery.getOuterQuery() != null) {
            throw NO_NESTED_OUTER_QUERY;
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

        if (outerQuery != null) {
            outerQuery.configure(config);
        }
    }

    @Override
    public String toString() {
        return "{tableFunction: " + tableFunction + ", projection: " + projection + ", filter: " + filter + ", aggregation: " + aggregation +
               ", postAggregations: " + postAggregations + ", window: " + window + ", duration: " + duration + ", outerQuery: " + outerQuery + "}";
    }
}
