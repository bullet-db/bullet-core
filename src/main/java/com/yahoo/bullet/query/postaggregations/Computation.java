/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.querying.postaggregations.ComputationStrategy;
import com.yahoo.bullet.querying.postaggregations.PostStrategy;
import com.yahoo.bullet.query.Field;
import lombok.Getter;

import java.util.List;

@Getter
public class Computation extends PostAggregation {
    private static final long serialVersionUID = -1401910210528780976L;

    public static final BulletException COMPUTATION_REQUIRES_FIELDS =
            new BulletException("The COMPUTATION post-aggregation requires at least one field.", "Please add at least one field.");

    private List<Field> fields;

    /**
     * Constructor that creates a Computation post-aggregation.
     *
     * @param fields The non-null list of fields to compute after aggregation.
     */
    public Computation(List<Field> fields) {
        super(PostAggregationType.COMPUTATION);
        Utilities.requireNonNull(fields);
        if (fields.isEmpty()) {
            throw COMPUTATION_REQUIRES_FIELDS;
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
