/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.BinaryExpression;
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
        this.left = Evaluator.build(binaryExpression.getLeft());
        this.right = Evaluator.build(binaryExpression.getRight());
        this.op = BINARY_OPERATORS.get(binaryExpression.getOp());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return cast(op.apply(left, right, record));
    }
}
