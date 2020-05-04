/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import static com.yahoo.bullet.querying.evaluators.Evaluator.NAryOperator;

public class NAryOperations {
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
