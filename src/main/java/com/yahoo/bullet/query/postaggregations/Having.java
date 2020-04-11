/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.postaggregations.HavingStrategy;
import com.yahoo.bullet.postaggregations.PostStrategy;
import com.yahoo.bullet.query.expressions.Expression;
import lombok.Getter;

import java.util.Objects;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter
public class Having extends PostAggregation {
    private static final long serialVersionUID = -123184459098221770L;

    public static final BulletError HAVING_REQUIRES_EXPRESSION =
            makeError("The HAVING post-aggregation requires an expression.", "Please add an expression.");

    private Expression expression;

    public Having(Expression expression) {
        super(Type.HAVING);
        this.expression = Objects.requireNonNull(expression);
    }

    @Override
    public PostStrategy getPostStrategy() {
        return new HavingStrategy(this);
    }
}
