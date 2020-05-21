/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.CastExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

/**
 * An evaluator that force casts the result of an evaluator to a given type.
 */
public class CastEvaluator extends Evaluator {
    final Evaluator value;
    final Type castType;

    /**
     * Constructor that creates a cast evaluator from a {@link CastExpression}.
     *
     * @param castExpression The cast expression to construct the evaluator from.
     */
    public CastEvaluator(CastExpression castExpression) {
        value = castExpression.getValue().getEvaluator();
        castType = castExpression.getCastType();
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return value.evaluate(record).forceCast(castType);
    }
}
