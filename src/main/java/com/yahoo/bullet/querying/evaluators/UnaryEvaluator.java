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

    public UnaryEvaluator(UnaryExpression unaryExpression) {
        super(unaryExpression);
        operand = unaryExpression.getOperand().getEvaluator();
        op = UnaryOperations.UNARY_OPERATORS.get(unaryExpression.getOp());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return op.apply(operand, record);
    }
}
