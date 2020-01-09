/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.ValueExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

/**
 * Need to make sure TypedObjects are properly immutable.
 */
public class ValueEvaluator extends Evaluator {
    private final TypedObject value;

    public ValueEvaluator(ValueExpression valueExpression) {
        super(valueExpression);
        this.value = new TypedObject(type, valueExpression.getValue());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return value;
    }
}
