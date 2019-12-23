/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.querying.Projection;
import com.yahoo.bullet.result.Clip;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class ComputationStrategy implements PostStrategy {
    private Computation computation;
    private Projection projection;

    public ComputationStrategy(Computation computation) {
        this.computation = computation;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        Optional<List<BulletError>> errors = computation.initialize();
        if (errors.isPresent()) {
            return errors;
        }
        projection = new Projection(computation.getProjection());
        return Optional.empty();
    }

    @Override
    public Clip execute(Clip clip) {
        clip.getRecords().forEach(projection::project);
        return clip;
    }

    @Override
    public Set<String> getRequiredFields() {
        return Collections.emptySet();
    }
}
