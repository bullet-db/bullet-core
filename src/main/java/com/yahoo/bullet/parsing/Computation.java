/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import lombok.Getter;
import lombok.Setter;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter @Setter
public class Computation extends PostAggregation {
    public static final BulletError COMPUTATION_REQUIRES_PROJECTION =
            makeError("The COMPUTATION post-aggregation requires a projection.", "Please add a projection.");

    @Expose
    private Projection projection;

    /**
     * Default constructor. GSON recommended
     */
    public Computation() {
        projection = null;
    }

    @Override
    public String toString() {
        return "{type: " + type + ", projection: " + projection + "}";
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        Optional<List<BulletError>> errors = super.initialize();
        if (errors.isPresent()) {
            return errors;
        }
        if (projection == null) {
            return Optional.of(Collections.singletonList(COMPUTATION_REQUIRES_PROJECTION));
        }
        return projection.initialize();
    }
}
