/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.parsing.PostAggregation;
import com.yahoo.bullet.postaggregations.Computation;
import com.yahoo.bullet.postaggregations.OrderBy;
import com.yahoo.bullet.postaggregations.PostStrategy;

public class PostAggregationOperations {
    /**
     * Returns a new {@link PostStrategy} instance that can handle this post aggregation.
     *
     * @param aggregation The non-null, initialized {@link PostAggregation} instance.
     *
     * @return The created instance of a post strategy that can implement the PostAggregation.
     */
    public static PostStrategy findPostStrategy(PostAggregation aggregation) {
        PostStrategy postStrategy = null;
        // Guaranteed to be present.
        switch (aggregation.getType()) {
            case ORDER_BY:
                postStrategy  = new OrderBy(aggregation);
                break;
            case COMPUTATION:
                postStrategy  = new Computation(aggregation);
                break;
        }
        return postStrategy;
    }
}
