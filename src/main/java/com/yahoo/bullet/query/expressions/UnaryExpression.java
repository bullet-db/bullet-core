/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.UnaryEvaluator;
import lombok.Getter;

import java.util.Objects;

/**
 * An expression that takes an operand and a unary operation. These fields are required; however, an optional
 * primitive type may be specified.
 */
@Getter
public class UnaryExpression extends Expression {
    private static final long serialVersionUID = -1893522779659725928L;

    private final Expression operand;
    private final Operation op;

    public UnaryExpression(Expression operand, Operation op) {
        this.operand = Objects.requireNonNull(operand);
        this.op = Objects.requireNonNull(op);
    }

    @Override
    public String getName() {
        return op + "(" + operand.getName() + ")";
    }

    @Override
    public Evaluator getEvaluator() {
        return new UnaryEvaluator(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof UnaryExpression)) {
            return false;
        }
        UnaryExpression other = (UnaryExpression) obj;
        return Objects.equals(operand, other.operand) && op == other.op && type == other.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(operand, op, type);
    }

    @Override
    public String toString() {
        return "{operand: " + operand + ", op: " + op + ", " + super.toString() + "}";
    }
}
