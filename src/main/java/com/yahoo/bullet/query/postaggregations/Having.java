/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.querying.postaggregations.HavingStrategy;
import com.yahoo.bullet.querying.postaggregations.PostStrategy;
import com.yahoo.bullet.query.expressions.Expression;
import lombok.Getter;

@Getter
public class Having extends PostAggregation {
    private static final long serialVersionUID = -123184459098221770L;

    public static final BulletException HAVING_REQUIRES_EXPRESSION =
            new BulletException("The HAVING post-aggregation requires an expression.", "Please add an expression.");

    private Expression expression;

    /**
     * Constructor that creates a Having post-aggregation.
     *
     * @param expression The non-null expression to filter by after aggregation.
     */
    public Having(Expression expression) {
        super(PostAggregationType.HAVING);
        if (expression == null) {
            throw HAVING_REQUIRES_EXPRESSION;
        }
        this.expression = expression;
    }

    @Override
    public PostStrategy getPostStrategy() {
        return new HavingStrategy(this);
    }

    @Override
    public String toString() {
        return "{type: " + type + ", expression: " + expression + "}";
    }
}
