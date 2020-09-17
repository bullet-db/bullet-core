/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.ValueExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

/**
 * An evaluator that returns a constant value.
 */
public class ValueEvaluator extends Evaluator {
    private static final long serialVersionUID = -1689526286716310223L;

    final TypedObject value;

    /**
     * Constructor that creates a value evaluator from a {@link ValueExpression}.
     *
     * @param valueExpression The value expression to construct the evaluator from.
     */
    public ValueEvaluator(ValueExpression valueExpression) {
        value = new TypedObject(valueExpression.getType(), valueExpression.getValue());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return value;
    }
}
