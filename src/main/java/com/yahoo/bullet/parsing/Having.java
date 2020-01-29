/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.expressions.Expression;
import lombok.Getter;
import lombok.Setter;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter @Setter
public class Having extends PostAggregation {
    public static final BulletError HAVING_REQUIRES_EXPRESSION =
            makeError("The HAVING post-aggregation requires an expression.", "Please add an expression.");

    @Expose
    private Expression expression;

    public Having(Expression expression) {
        this.expression = expression;
        this.type = Type.HAVING;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        Optional<List<BulletError>> errors = super.initialize();
        if (errors.isPresent()) {
            return errors;
        }
        if (expression == null) {
            return Optional.of(Collections.singletonList(HAVING_REQUIRES_EXPRESSION));
        }
        return expression.initialize();
    }
}
