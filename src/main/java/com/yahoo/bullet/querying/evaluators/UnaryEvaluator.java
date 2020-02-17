/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.UnaryExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

/**
 * Evaluator that evaluates the operand before applying a unary operator. Casts the result.
 */
public class UnaryEvaluator extends Evaluator {
    private Evaluator operand;
    private UnaryOperator op;

    public UnaryEvaluator(UnaryExpression unaryExpression) {
        super(unaryExpression);
        operand = unaryExpression.getOperand().getEvaluator();
        op = UNARY_OPERATORS.get(unaryExpression.getOp());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return op.apply(operand, record);
    }
}
