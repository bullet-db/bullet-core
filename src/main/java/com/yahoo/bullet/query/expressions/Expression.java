/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.typesystem.Type;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * The supported expressions are:
 * - ValueExpression
 * - FieldExpression
 * - UnaryExpression
 * - BinaryExpression
 * - NAryExpression
 * - ListExpression
 * - CastExpression
 *
 * Look at {@link Evaluator} to see how expressions are evaluated.
 */
@Getter @Setter
public abstract class Expression implements Serializable {
    private static final long serialVersionUID = -769774785327135375L;

    protected Type type;

    /**
     * Constructs an evaluator for this expression and returns it.
     *
     * @return A newly-constructed evaluator for this expression.
     */
    public abstract Evaluator getEvaluator();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract int hashCode();

    @Override
    public String toString() {
        return "type: " + type;
    }
}
