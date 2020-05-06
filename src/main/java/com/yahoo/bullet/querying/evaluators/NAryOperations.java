/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.List;

public class NAryOperations {
    @FunctionalInterface
    public interface NAryOperator {
        TypedObject apply(List<Evaluator> evaluator, BulletRecord record);
    }

    static NAryOperator ALL_MATCH = (evaluators, record) -> {
        for (Evaluator evaluator : evaluators) {
            TypedObject value = evaluator.evaluate(record);
            if (!((Boolean) value.getValue())) {
                return new TypedObject(Type.BOOLEAN, false);
            }
        }
        return new TypedObject(Type.BOOLEAN, true);
    };

    static NAryOperator ANY_MATCH = (evaluators, record) -> {
        for (Evaluator evaluator : evaluators) {
            TypedObject value = evaluator.evaluate(record);
            if ((Boolean) value.getValue()) {
                return new TypedObject(Type.BOOLEAN, true);
            }
        }
        return new TypedObject(Type.BOOLEAN, false);
    };

    static NAryOperator IF = (evaluators, record) -> {
        TypedObject condition = evaluators.get(0).evaluate(record);
        return (Boolean) condition.getValue() ? evaluators.get(1).evaluate(record) : evaluators.get(2).evaluate(record);
    };
}
