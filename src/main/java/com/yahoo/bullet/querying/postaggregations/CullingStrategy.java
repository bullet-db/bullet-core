/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.postaggregations;

import com.yahoo.bullet.query.postaggregations.Culling;
import com.yahoo.bullet.result.Clip;

import java.util.Set;

public class CullingStrategy implements PostStrategy {
    private Set<String> transientFields;

    /**
     * Constructor that creates a Culling post-strategy.
     *
     * @param culling The culling post-aggregation to create a strategy for.
     */
    public CullingStrategy(Culling culling) {
        transientFields = culling.getTransientFields();
    }

    @Override
    public Clip execute(Clip clip) {
        for (String field : transientFields) {
            clip.getRecords().forEach(record -> record.remove(field));
        }
        return clip;
    }
}
