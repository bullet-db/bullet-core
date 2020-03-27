/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.query.expressions.Expression;
import lombok.Getter;
import lombok.Setter;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter @Setter
public class Having extends PostAggregation {
    public static final BulletError HAVING_REQUIRES_EXPRESSION =
            makeError("The HAVING post-aggregation requires an expression.", "Please add an expression.");

    private Expression expression;

    public Having(Expression expression) {
        this.expression = expression;
        this.type = Type.HAVING;
    }
}
