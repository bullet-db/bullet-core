/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.Field;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter @Setter
public class Computation extends PostAggregation {
    public static final BulletError COMPUTATION_REQUIRES_FIELDS =
            makeError("The COMPUTATION post-aggregation requires at least one field.", "Please add fields.");

    private List<Field> fields;

    public Computation() {
        type = Type.COMPUTATION;
    }

    public Computation(List<Field> fields) {
        this.fields = fields;
        this.type = Type.COMPUTATION;
    }

    @Override
    public String toString() {
        return "{type: " + type + ", fields: " + fields + "}";
    }
}
