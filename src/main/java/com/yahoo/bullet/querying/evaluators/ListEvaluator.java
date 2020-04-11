/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
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
 * Evaluator that evaluates a list of evaluators on a BulletRecord and then returns the list of results.
 */
public class ListEvaluator extends Evaluator {
    private List<Evaluator> evaluators;

    public ListEvaluator(ListExpression listExpression) {
        super(listExpression);
        evaluators = listExpression.getValues().stream().map(Expression::getEvaluator).collect(Collectors.toList());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return new TypedObject(evaluators.stream().map(e -> e.evaluate(record).getValue()).collect(Collectors.toCollection(ArrayList::new)));
    }
}
