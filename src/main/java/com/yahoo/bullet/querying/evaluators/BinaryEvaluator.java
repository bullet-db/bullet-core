/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

/**
 * An evaluator that applies a binary operator to the result of a left evaluator and the result of a right evaluator.
 */
public class BinaryEvaluator extends Evaluator {
    private static final long serialVersionUID = -467853226398830498L;

    final Evaluator left;
    final Evaluator right;
    final BinaryOperations.BinaryOperator op;

    /**
     * Constructor that creates a binary evaluator from a {@link BinaryExpression}.
     *
     * @param binaryExpression The binary expression to construct the evaluator from.
     */
    public BinaryEvaluator(BinaryExpression binaryExpression) {
        left = binaryExpression.getLeft().getEvaluator();
        right = binaryExpression.getRight().getEvaluator();
        op = BinaryOperations.BINARY_OPERATORS.get(binaryExpression.getOp());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return op.apply(left, right, record);
    }
}
