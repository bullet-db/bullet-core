/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.Configurable;
import com.yahoo.bullet.common.Initializable;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * This class is the top level Bullet Query specification. It holds the definition of the Query.
 */
@Getter @Setter(AccessLevel.PACKAGE) @Slf4j
public class Query implements Configurable, Initializable {
    @Expose
    private Projection projection;
    @Expose
    private List<Clause> filters;
    @Expose
    private Aggregation aggregation;
    @Expose
    private Window window;
    @Expose
    private Integer duration;

    /**
     * Default constructor. GSON recommended.
     */
    public Query() {
        filters = null;
        // If no aggregation is provided, the default one is used. Aggregations must be present.
        aggregation = new Aggregation();
    }


    @Override
    @SuppressWarnings("unchecked")
    public void configure(BulletConfig config) {
        if (filters != null) {
            filters.forEach(f -> f.configure(config));
        }
        if (projection != null) {
            projection.configure(config);
        }
        // Must have an aggregation
        if (aggregation == null) {
            aggregation = new Aggregation();
        }
        aggregation.configure(config);

        int durationDefault = config.getAs(BulletConfig.SPECIFICATION_DEFAULT_DURATION, Integer.class);
        int durationMax = config.getAs(BulletConfig.SPECIFICATION_MAX_DURATION, Integer.class);

        // Null or negative, then default, else min of duration and max.
        duration = (duration == null || duration < 0) ? durationDefault : Math.min(duration, durationMax);
    }

    @Override
    public Optional<List<Error>> initialize() {
        List<Error> errors = new ArrayList<>();
        if (filters != null) {
            for (Clause clause : filters) {
                clause.initialize().ifPresent(errors::addAll);
            }
        }
        if (projection != null) {
            projection.initialize().ifPresent(errors::addAll);
        }
        if (aggregation != null) {
            aggregation.initialize().ifPresent(errors::addAll);
        }
        return errors.isEmpty() ? Optional.empty() : Optional.of(errors);
    }


    @Override
    public String toString() {
        return "{filters: " + filters + ", projection: " + projection + ", aggregation: " + aggregation +
               ", duration: " + duration + "}";
    }
}
