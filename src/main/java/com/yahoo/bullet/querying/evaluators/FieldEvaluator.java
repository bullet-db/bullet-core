/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.Expression;
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
        final Serializable key = fieldExpression.getKey();
        final Serializable subKey = fieldExpression.getSubKey();

        if (key instanceof String) {
            if (subKey instanceof String) {
                return record -> record.typedGet(field, (String) key, (String) subKey);
            } else if (subKey instanceof Expression) {
                final Evaluator subKeyEvaluator = ((Expression) subKey).getEvaluator();
                return record -> {
                    TypedObject subKeyArg = subKeyEvaluator.evaluate(record);
                    if (subKeyArg.isNull()) {
                        return TypedObject.NULL;
                    }
                    return record.typedGet(field, (String) key, (String) subKeyArg.getValue());
                };
            } else {
                return record -> record.typedGet(field, (String) key);
            }
        } else if (key instanceof Integer) {
            if (subKey instanceof String) {
                return record -> record.typedGet(field, (Integer) key, (String) subKey);
            } else if (subKey instanceof Expression) {
                final Evaluator subKeyEvaluator = ((Expression) subKey).getEvaluator();
                return record -> {
                    TypedObject subKeyArg = subKeyEvaluator.evaluate(record);
                    if (subKeyArg.isNull()) {
                        return TypedObject.NULL;
                    }
                    return record.typedGet(field, (Integer) key, (String) subKeyArg.getValue());
                };
            } else {
                return record -> record.typedGet(field, (Integer) key);
            }
        } else if (key instanceof Expression) {
            final Evaluator keyEvaluator = ((Expression) key).getEvaluator();
            if (subKey instanceof String) {
                return record -> {
                    TypedObject keyArg = keyEvaluator.evaluate(record);
                    if (keyArg.isNull()) {
                        return TypedObject.NULL;
                    }
                    Type type = keyArg.getType();
                    if (Type.isNumeric(type)) {
                        return record.typedGet(field, ((Number) keyArg.getValue()).intValue(), (String) subKey);
                    } else {
                        return record.typedGet(field, (String) keyArg.getValue(), (String) subKey);
                    }
                };
            } else if (subKey instanceof Expression) {
                final Evaluator subKeyEvaluator = ((Expression) subKey).getEvaluator();
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
