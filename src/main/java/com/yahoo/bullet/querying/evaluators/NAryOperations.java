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

import java.io.Serializable;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class NAryOperations {
    @FunctionalInterface
    public interface NAryOperator extends Serializable {
        TypedObject apply(List<Evaluator> evaluator, BulletRecord record);
    }

    static final Map<Operation, NAryOperator> N_ARY_OPERATORS = new EnumMap<>(Operation.class);

    static {
        N_ARY_OPERATORS.put(Operation.AND, NAryOperations::allMatch);
        N_ARY_OPERATORS.put(Operation.OR, NAryOperations::anyMatch);
        N_ARY_OPERATORS.put(Operation.IF, NAryOperations::ternary);
    }

    static TypedObject allMatch(List<Evaluator> evaluators, BulletRecord record) {
        boolean hasNull = false;
        for (Evaluator evaluator : evaluators) {
            TypedObject value = evaluator.evaluate(record);
            if (value.isNull()) {
                hasNull = true;
            } else if (!((Boolean) value.forceCast(Type.BOOLEAN).getValue())) {
                return new TypedObject(Type.BOOLEAN, false);
            }
        }
        return !hasNull ? new TypedObject(Type.BOOLEAN, true) : TypedObject.NULL;
    };

    static TypedObject anyMatch(List<Evaluator> evaluators, BulletRecord record) {
        boolean hasNull = false;
        for (Evaluator evaluator : evaluators) {
            TypedObject value = evaluator.evaluate(record);
            if (value.isNull()) {
                hasNull = true;
            } else if ((Boolean) value.forceCast(Type.BOOLEAN).getValue()) {
                return new TypedObject(Type.BOOLEAN, true);
            }
        }
        return !hasNull ? new TypedObject(Type.BOOLEAN, false) : TypedObject.NULL;
    };

    static TypedObject ternary(List<Evaluator> evaluators, BulletRecord record) {
        TypedObject condition = evaluators.get(0).evaluate(record);
        return !condition.isNull() && (Boolean) condition.getValue() ? evaluators.get(1).evaluate(record) :
                                                                       evaluators.get(2).evaluate(record);
    };
}
