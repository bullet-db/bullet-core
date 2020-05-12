/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.expressions;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.UnaryEvaluator;
import lombok.Getter;

import java.util.Objects;

import static com.yahoo.bullet.query.expressions.Operation.UNARY_OPERATIONS;

/**
 * An expression that requires an operand and a unary operation.
 */
@Getter
public class UnaryExpression extends Expression {
    private static final long serialVersionUID = -1893522779659725928L;
    private static final BulletException UNARY_EXPRESSION_REQUIRES_UNARY_OPERATION =
            new BulletException("Unary expression requires a unary operation.", "Please specify a unary operation.");

    private final Expression operand;
    private final Operation op;

    public UnaryExpression(Expression operand, Operation op) {
        this.operand = Objects.requireNonNull(operand);
        this.op = Objects.requireNonNull(op);
        if (!UNARY_OPERATIONS.contains(op)) {
            throw UNARY_EXPRESSION_REQUIRES_UNARY_OPERATION;
        }
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
