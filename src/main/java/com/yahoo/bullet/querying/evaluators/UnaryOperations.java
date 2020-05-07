/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.EnumMap;
import java.util.Map;

/**
 * Unary operations used by UnaryEvaluator.
 */
public class UnaryOperations {
    @FunctionalInterface
    public interface UnaryOperator {
        TypedObject apply(Evaluator evaluator, BulletRecord record);
    }

    public static final Map<Operation, UnaryOperator> UNARY_OPERATORS = new EnumMap<>(Operation.class);

    static {
        UNARY_OPERATORS.put(Operation.NOT, UnaryOperations.NOT);
        UNARY_OPERATORS.put(Operation.SIZE_OF, UnaryOperations.SIZE_OF);
        UNARY_OPERATORS.put(Operation.IS_NULL, UnaryOperations.IS_NULL);
        UNARY_OPERATORS.put(Operation.IS_NOT_NULL, UnaryOperations.IS_NOT_NULL);
    }

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
