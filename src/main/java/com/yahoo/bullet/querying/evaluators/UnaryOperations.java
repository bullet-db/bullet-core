/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import static com.yahoo.bullet.querying.evaluators.Evaluator.UnaryOperator;

/**
 * Unary operations used by UnaryEvaluator.
 */
public class UnaryOperations {
    static UnaryOperator NOT = (evaluator, record) -> {
        TypedObject value = evaluator.evaluate(record);
        return new TypedObject(Type.BOOLEAN, !((Boolean) value.forceCast(Type.BOOLEAN).getValue()));
    };

    static UnaryOperator SIZE_OF = (evaluator, record) -> {
        TypedObject value = evaluator.evaluate(record);
        return new TypedObject(Type.INTEGER, value.size());
    };

    static UnaryOperator IS_NULL = (evaluator, record) ->
            new TypedObject(Type.BOOLEAN, Type.isNull(evaluator.evaluate(record).getType()));

    static UnaryOperator IS_NOT_NULL = (evaluator, record) ->
            new TypedObject(Type.BOOLEAN, !Type.isNull(evaluator.evaluate(record).getType()));
}
