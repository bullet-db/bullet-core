/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.querying.postaggregations.PostStrategy;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

@Getter @AllArgsConstructor
public abstract class PostAggregation implements Serializable {
    private static final long serialVersionUID = -3083946184345104820L;

    /** Represents the type of the PostAggregation. */
    @Getter @AllArgsConstructor
    public enum Type {
        HAVING(0),
        COMPUTATION(1),
        ORDER_BY(2),
        CULLING(3);

        private int priority;
    }

    protected final Type type;

    public abstract PostStrategy getPostStrategy();
}
