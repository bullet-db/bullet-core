/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.querying.postaggregations.CullingStrategy;
import com.yahoo.bullet.querying.postaggregations.PostStrategy;
import lombok.Getter;

import java.util.Set;

@Getter
public class Culling extends PostAggregation {
    private static final long serialVersionUID = -4606818164037391850L;

    public static final BulletException CULLING_REQUIRES_FIELDS =
            new BulletException("The CULLING post-aggregation requires at least one field.", "Please add at least one field.");

    private Set<String> transientFields;

    /**
     * Constructor that creates a Culling post-aggregation.
     *
     * @param transientFields The non-null set of fields to remove after aggregation.
     */
    public Culling(Set<String> transientFields) {
        super(PostAggregationType.CULLING);
        Utilities.requireNonNull(transientFields);
        if (transientFields.isEmpty()) {
            throw CULLING_REQUIRES_FIELDS;
        }
        this.transientFields = transientFields;
    }

    @Override
    public PostStrategy getPostStrategy() {
        return new CullingStrategy(this);
    }

    @Override
    public String toString() {
        return "{type: " + type + ", transientFields: " + transientFields + "}";
    }
}
