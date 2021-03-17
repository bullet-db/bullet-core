/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.io.Serializable;

/**
 * An evaluator that extracts a given field from a {@link BulletRecord}. This is the only evaluator that directly takes a
 * {@link BulletRecord}.
 */
public class FieldEvaluator extends Evaluator {
    private static final long serialVersionUID = -1186787768122072138L;

    @FunctionalInterface
    public interface FieldExtractor extends Serializable {
        TypedObject extract(BulletRecord record);
    }

    private final FieldExtractor fieldExtractor;

    /**
     * Constructor that creates a field evaluator from a {@link FieldExpression}.
     *
     * @param fieldExpression The field expression to construct the evaluator from.
     */
    public FieldEvaluator(FieldExpression fieldExpression) {
        fieldExtractor = getFieldExtractor(fieldExpression);
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return fieldExtractor.extract(record);
    }

    private static FieldExtractor getFieldExtractor(FieldExpression fieldExpression) {
        final String field = fieldExpression.getField();
        final Integer index = fieldExpression.getIndex();
        final String key = fieldExpression.getKey();
        final String subKey = fieldExpression.getSubKey();
        final Evaluator keyEvaluator = fieldExpression.getVariableKey() != null ? fieldExpression.getVariableKey().getEvaluator() : null;
        final Evaluator subKeyEvaluator = fieldExpression.getVariableSubKey() != null ? fieldExpression.getVariableSubKey().getEvaluator() : null;
        if (index != null) {
            if (subKey != null) {
                return record -> record.typedGet(field, index, subKey);
            } else if (subKeyEvaluator != null) {
                return record -> {
                    TypedObject subKeyArg = subKeyEvaluator.evaluate(record);
                    if (subKeyArg.isNull()) {
                        return TypedObject.NULL;
                    }
                    return record.typedGet(field, index, (String) subKeyArg.getValue());
                };
            } else {
                return record -> record.typedGet(field, index);
            }
        } else if (key != null) {
            if (subKey != null) {
                return record -> record.typedGet(field, key, subKey);
            } else if (subKeyEvaluator != null) {
                return record -> {
                    TypedObject subKeyArg = subKeyEvaluator.evaluate(record);
                    if (subKeyArg.isNull()) {
                        return TypedObject.NULL;
                    }
                    return record.typedGet(field, key, (String) subKeyArg.getValue());
                };
            } else {
                return record -> record.typedGet(field, key);
            }
        } else if (keyEvaluator != null) {
            if (subKey != null) {
                return record -> {
                    TypedObject keyArg = keyEvaluator.evaluate(record);
                    if (keyArg.isNull()) {
                        return TypedObject.NULL;
                    }
                    Type type = keyArg.getType();
                    if (Type.isNumeric(type)) {
                        return record.typedGet(field, ((Number) keyArg.getValue()).intValue(), subKey);
                    } else {
                        return record.typedGet(field, (String) keyArg.getValue(), subKey);
                    }
                };
            } else if (subKeyEvaluator != null) {
                return record -> {
                    TypedObject keyArg = keyEvaluator.evaluate(record);
                    if (keyArg.isNull()) {
                        return TypedObject.NULL;
                    }
                    TypedObject subKeyArg = subKeyEvaluator.evaluate(record);
                    if (subKeyArg.isNull()) {
                        return TypedObject.NULL;
                    }
                    Type type = keyArg.getType();
                    if (Type.isNumeric(type)) {
                        return record.typedGet(field, ((Number) keyArg.getValue()).intValue(), (String) subKeyArg.getValue());
                    } else {
                        return record.typedGet(field, (String) keyArg.getValue(), (String) subKeyArg.getValue());
                    }
                };
            } else {
                return record -> {
                    TypedObject keyArg = keyEvaluator.evaluate(record);
                    if (keyArg.isNull()) {
                        return TypedObject.NULL;
                    }
                    Type type = keyArg.getType();
                    if (Type.isNumeric(type)) {
                        return record.typedGet(field, ((Number) keyArg.getValue()).intValue());
                    } else {
                        return record.typedGet(field, (String) keyArg.getValue());
                    }
                };
            }
        } else {
            return record -> record.typedGet(field);
        }
    }
}
