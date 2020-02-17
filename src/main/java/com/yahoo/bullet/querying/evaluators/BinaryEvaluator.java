/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.BinaryExpression;
import com.yahoo.bullet.parsing.expressions.BinaryExpression.Modifier;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

/**
 * Evaluator that evaluates the left and right before applying a binary operator. Casts the result.
 */
public class BinaryEvaluator extends Evaluator {
    private Evaluator left;
    private Evaluator right;
    private BinaryOperator op;

    public BinaryEvaluator(BinaryExpression binaryExpression) {
        super(binaryExpression);
        left = binaryExpression.getLeft().getEvaluator();
        right = binaryExpression.getRight().getEvaluator();
        Modifier modifier = binaryExpression.getModifier();
        if (modifier == null) {
            op = BINARY_OPERATORS.get(binaryExpression.getOp());
        } else if (modifier == Modifier.ANY) {
            op = BINARY_ANY_OPERATORS.get(binaryExpression.getOp());
        } else {
            op = BINARY_ALL_OPERATORS.get(binaryExpression.getOp());
        }
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return op.apply(left, right, record);
    }
}
