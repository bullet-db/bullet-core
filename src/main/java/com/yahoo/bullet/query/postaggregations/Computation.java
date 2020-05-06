/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.postaggregations.ComputationStrategy;
import com.yahoo.bullet.postaggregations.PostStrategy;
import com.yahoo.bullet.query.Field;
import lombok.Getter;

import java.util.List;

@Getter
public class Computation extends PostAggregation {
    private static final long serialVersionUID = -1401910210528780976L;

    public static final BulletError COMPUTATION_REQUIRES_FIELDS =
            new BulletError("The COMPUTATION post-aggregation requires at least one field.", "Please add at least one field.");

    private List<Field> fields;

    public Computation(List<Field> fields) {
        super(Type.COMPUTATION);
        Utilities.requireNonNull(fields);
        if (fields.isEmpty()) {
            throw new BulletException(COMPUTATION_REQUIRES_FIELDS);
        }
        this.fields = fields;
    }

    @Override
    public PostStrategy getPostStrategy() {
        return new ComputationStrategy(this);
    }

    @Override
    public String toString() {
        return "{type: " + type + ", fields: " + fields + "}";
    }
}
