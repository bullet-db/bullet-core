/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.UnaryExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

/**
 * An evaluator that applies a unary operator to the result of an evaluator.
 */
public class UnaryEvaluator extends Evaluator {
    final Evaluator operand;
    final UnaryOperations.UnaryOperator op;

    /**
     * Constructor that creates a unary evaluator from a {@link UnaryExpression}.
     *
     * @param unaryExpression The unary expression to construct the evaluator from.
     */
    public UnaryEvaluator(UnaryExpression unaryExpression) {
        operand = unaryExpression.getOperand().getEvaluator();
        op = UnaryOperations.UNARY_OPERATORS.get(unaryExpression.getOp());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return op.apply(operand, record);
    }
}
