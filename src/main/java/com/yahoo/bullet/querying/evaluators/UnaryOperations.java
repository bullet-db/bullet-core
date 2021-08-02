/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.io.Serializable;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Unary operations used by UnaryEvaluator.
 */
public class UnaryOperations {
    @FunctionalInterface
    public interface UnaryOperator extends Serializable {
        TypedObject apply(Evaluator evaluator, BulletRecord record);
    }

    static final Map<Operation, UnaryOperator> UNARY_OPERATORS = new EnumMap<>(Operation.class);

    static {
        UNARY_OPERATORS.put(Operation.NOT, UnaryOperations::not);
        UNARY_OPERATORS.put(Operation.SIZE_OF, UnaryOperations::sizeOf);
        UNARY_OPERATORS.put(Operation.IS_NULL, UnaryOperations::isNull);
        UNARY_OPERATORS.put(Operation.IS_NOT_NULL, UnaryOperations::isNotNull);
        UNARY_OPERATORS.put(Operation.TRIM, UnaryOperations::trim);
        UNARY_OPERATORS.put(Operation.ABS, UnaryOperations::abs);
        UNARY_OPERATORS.put(Operation.LOWER, UnaryOperations::lower);
        UNARY_OPERATORS.put(Operation.UPPER, UnaryOperations::upper);
    }

    static TypedObject not(Evaluator evaluator, BulletRecord record) {
        return checkNull(evaluator, record, value -> TypedObject.valueOf(!((Boolean) value.forceCast(Type.BOOLEAN).getValue())));
    }

    static TypedObject sizeOf(Evaluator evaluator, BulletRecord record) {
        return checkNull(evaluator, record, value -> TypedObject.valueOf(value.size()));
    }

    static TypedObject isNull(Evaluator evaluator, BulletRecord record) {
        return TypedObject.valueOf(Utilities.isNull(evaluator.evaluate(record)));
    }

    static TypedObject isNotNull(Evaluator evaluator, BulletRecord record) {
        return TypedObject.valueOf(!Utilities.isNull(evaluator.evaluate(record)));
    }

    static TypedObject trim(Evaluator evaluator, BulletRecord record) {
        return checkNull(evaluator, record, value -> {
            String str = (String) value.getValue();
            return TypedObject.valueOf(str.trim());
        });
    }

    static TypedObject lower(Evaluator evaluator, BulletRecord record) {
        return checkNull(evaluator, record, value -> {
            String str = (String) value.getValue();
            return TypedObject.valueOf(str.toLowerCase());
        });
    }

    static TypedObject upper(Evaluator evaluator, BulletRecord record) {
        return checkNull(evaluator, record, value -> {
            String str = (String) value.getValue();
            return TypedObject.valueOf(str.toUpperCase());
        });
    }

    static TypedObject abs(Evaluator evaluator, BulletRecord record) {
        return checkNull(evaluator, record, value -> {
            Number number = (Number) value.getValue();
            switch (value.getType()) {
                case DOUBLE:
                    return TypedObject.valueOf(Math.abs(number.doubleValue()));
                case FLOAT:
                    return TypedObject.valueOf(Math.abs(number.floatValue()));
                case LONG:
                    return TypedObject.valueOf(Math.abs(number.longValue()));
                default:
                    return TypedObject.valueOf(Math.abs(number.intValue()));
            }
        });
    }

    private static TypedObject checkNull(Evaluator evaluator, BulletRecord record, Function<TypedObject, TypedObject> operator) {
        TypedObject value = evaluator.evaluate(record);
        if (Utilities.isNull(value)) {
            return TypedObject.NULL;
        }
        return operator.apply(value);
    }
}
