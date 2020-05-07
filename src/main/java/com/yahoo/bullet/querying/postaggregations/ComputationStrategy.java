/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.postaggregations;

import com.yahoo.bullet.query.postaggregations.Computation;
import com.yahoo.bullet.querying.Projection;
import com.yahoo.bullet.result.Clip;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ComputationStrategy implements PostStrategy {
    private Projection projection;

    /**
     * Constructor for Computation strategy.
     *
     * @param computation Computation post aggregation.
     */
    public ComputationStrategy(Computation computation) {
        projection = new Projection(computation.getFields());
    }

    @Override
    public Clip execute(Clip clip) {
        clip.getRecords().forEach(projection::project);
        return clip;
    }
}
