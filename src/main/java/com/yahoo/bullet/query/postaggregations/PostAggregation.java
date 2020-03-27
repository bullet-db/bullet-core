/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public abstract class PostAggregation {
    /** Represents the type of the PostAggregation. */
    public enum Type {
        HAVING,
        COMPUTATION,
        ORDER_BY,
        CULLING
    }

    protected Type type;
}
