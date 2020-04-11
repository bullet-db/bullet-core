/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.postaggregations.ComputationStrategy;
import com.yahoo.bullet.postaggregations.PostStrategy;
import com.yahoo.bullet.query.Field;
import lombok.Getter;

import java.util.List;

@Getter
public class Computation extends PostAggregation {
    private static final long serialVersionUID = -1401910210528780976L;

    private List<Field> fields;

    public Computation(List<Field> fields) {
        super(Type.COMPUTATION);
        Utilities.requireNonNullList(fields);
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("List empty bad");
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
