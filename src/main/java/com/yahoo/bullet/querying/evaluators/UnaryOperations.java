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
        UNARY_OPERATORS.put(Operation.NOT, UnaryOperations::not);
        UNARY_OPERATORS.put(Operation.SIZE_OF, UnaryOperations::sizeOf);
        UNARY_OPERATORS.put(Operation.IS_NULL, UnaryOperations::isNull);
        UNARY_OPERATORS.put(Operation.IS_NOT_NULL, UnaryOperations::isNotNull);
    }

    static TypedObject not(Evaluator evaluator, BulletRecord record) {
        TypedObject value = evaluator.evaluate(record);
        return new TypedObject(Type.BOOLEAN, !((Boolean) value.forceCast(Type.BOOLEAN).getValue()));
    };

    static TypedObject sizeOf(Evaluator evaluator, BulletRecord record) {
        TypedObject value = evaluator.evaluate(record);
        return new TypedObject(Type.INTEGER, value.size());
    };

    static TypedObject isNull(Evaluator evaluator, BulletRecord record) {
        return new TypedObject(Type.BOOLEAN, evaluator.evaluate(record).isNull());
    }

    static TypedObject isNotNull(Evaluator evaluator, BulletRecord record) {
        return new TypedObject(Type.BOOLEAN, !evaluator.evaluate(record).isNull());
    }
}
