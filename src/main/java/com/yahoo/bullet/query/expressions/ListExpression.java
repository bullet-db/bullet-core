/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.ListEvaluator;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * An expression that holds a list of expressions. A primitive type
 * must be specified as only lists of primitives are supported at the moment.
 */
@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class ListExpression extends Expression {
    private static final String DELIMITER = ", ";

    private List<Expression> values;

    @Override
    public String getName() {
        return "[" + values.stream().map(Expression::getName).collect(Collectors.joining(DELIMITER)) + "]";
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
