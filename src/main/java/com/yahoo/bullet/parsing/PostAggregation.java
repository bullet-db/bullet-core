/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.parsing;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public abstract class PostAggregation {
    /** Represents the type of the PostAggregation. */
    public enum Type {
        ORDER_BY,
        COMPUTATION,
        HAVING,
        CULLING
    }

    protected Type type;
}
