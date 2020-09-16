/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.NAryExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.List;
import java.util.stream.Collectors;

/**
 * An evaluator that applies an n-ary operator to the results of a list of evaluators.
 */
public class NAryEvaluator extends Evaluator {
    private static final long serialVersionUID = 54879052369401372L;

    final List<Evaluator> operands;
    final NAryOperations.NAryOperator op;

    /**
     * Constructor that creates an n-ary evaluator from a {@link NAryExpression}.
     *
     * @param nAryExpression The n-ary expression to construct the evaluator from.
     */
    public NAryEvaluator(NAryExpression nAryExpression) {
        operands = nAryExpression.getOperands().stream().map(Expression::getEvaluator).collect(Collectors.toList());
        op = NAryOperations.N_ARY_OPERATORS.get(nAryExpression.getOp());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return op.apply(operands, record);
    }
}
