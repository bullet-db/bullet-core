/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.expressions.ListExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An evaluator that returns a list of the results of a list of evaluators.
 */
public class ListEvaluator extends Evaluator {
    final List<Evaluator> evaluators;

    public ListEvaluator(ListExpression listExpression) {
        super(listExpression);
        evaluators = listExpression.getValues().stream().map(Expression::getEvaluator).collect(Collectors.toList());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return new TypedObject(evaluators.stream().map(e -> e.evaluate(record).getValue()).collect(Collectors.toCollection(ArrayList::new)));
    }
}
