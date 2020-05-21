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

    protected final PostAggregationType type;

    /**
     * Returns a new {@link PostStrategy} that handles this post-aggregation.

     * @return A new instance of a strategy that handles this post-aggregation.
     */
    public abstract PostStrategy getPostStrategy();
}
