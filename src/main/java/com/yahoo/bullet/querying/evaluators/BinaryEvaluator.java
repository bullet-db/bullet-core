/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.BinaryExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.AccessLevel;
import lombok.Getter;

/**
 * Evaluator that evaluates the left and right before applying a binary operator. Casts the result.
 */
// For testing only
@Getter(AccessLevel.PACKAGE)
public class BinaryEvaluator extends Evaluator {
    private Evaluator left;
    private Evaluator right;
    private BinaryOperator op;

    public BinaryEvaluator(BinaryExpression binaryExpression) {
        super(binaryExpression);
        left = binaryExpression.getLeft().getEvaluator();
        right = binaryExpression.getRight().getEvaluator();
        switch (binaryExpression.getModifier()) {
            case ANY:
                op = BINARY_ANY_OPERATORS.get(binaryExpression.getOp());
                break;
            case ALL:
                op = BINARY_ALL_OPERATORS.get(binaryExpression.getOp());
                break;
            default:
                op = BINARY_OPERATORS.get(binaryExpression.getOp());
                break;
        }
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return op.apply(left, right, record);
    }
}
