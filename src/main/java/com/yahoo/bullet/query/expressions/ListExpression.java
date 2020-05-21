/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.ListEvaluator;
import lombok.Getter;

import java.util.List;
import java.util.Objects;

/**
 * An expression that holds a list of expressions.
 */
@Getter
public class ListExpression extends Expression {
    private static final long serialVersionUID = 311789452858823415L;

    private final List<Expression> values;

    /**
     * Constructor that creates a list expression.
     *
     * @param values The non-null list of values to be wrapped.
     */
    public ListExpression(List<Expression> values) {
        this.values = Utilities.requireNonNull(values);
    }

    @Override
    public Evaluator getEvaluator() {
        return new ListEvaluator(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ListExpression)) {
            return false;
        }
        ListExpression other = (ListExpression) obj;
        return Objects.equals(values, other.values) && type == other.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(values, type);
    }

    @Override
    public String toString() {
        return "{values: " + values + ", " + super.toString() + "}";
    }
}
