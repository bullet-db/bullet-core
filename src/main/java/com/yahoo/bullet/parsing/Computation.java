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
    @Expose
    private Expression expression;
    @Expose
    private String newName;

    public static final BulletError COMPUTATION_REQUIRES_VALID_EXPRESSION_ERROR =
            makeError("The COMPUTATION post aggregation needs a valid expression field", "Please add a valid expression.");
    public static final BulletError COMPUTATION_REQUIRES_NEW_FIELD_ERROR =
            makeError("The COMPUTATION post aggregation needs a non-empty new field name", "Please provide a non-empty new field name.");

    /**
     * Default constructor. GSON recommended
     */
    public Computation() {
        expression = null;
    }

    @Override
    public String toString() {
        return "{type: " + type + ", expression: " + expression + ", newName: " + newName + "}";
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        Optional<List<BulletError>> errors = super.initialize();
        if (errors.isPresent()) {
            return errors;
        }
        if (expression == null) {
            return Optional.of(Collections.singletonList(COMPUTATION_REQUIRES_VALID_EXPRESSION_ERROR));
        }
        errors = expression.initialize();
        if (errors.isPresent()) {
            return errors;
        }
        if (newName == null || newName.isEmpty()) {
            return Optional.of(Collections.singletonList(COMPUTATION_REQUIRES_NEW_FIELD_ERROR));
        }
        return Optional.empty();
    }
}
