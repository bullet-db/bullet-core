/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.ComplexFieldExpression;
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

    /**
     * Constructor that creates a field evaluator from a {@link ComplexFieldExpression}.
     *
     * @param fieldExpression The field expression to construct the evaluator from.
     */
    public FieldEvaluator(ComplexFieldExpression fieldExpression) {
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
        if (index != null) {
            if (subKey != null) {
                return record -> record.typedGet(field, index, subKey);
            }
            return record -> record.typedGet(field, index);
        } else if (key != null) {
            if (subKey != null) {
                return record -> record.typedGet(field, key, subKey);
            }
            return record -> record.typedGet(field, key);
        } else {
            return record -> record.typedGet(field);
        }
    }

    private static FieldExtractor getFieldExtractor(ComplexFieldExpression fieldExpression) {
        final String field = fieldExpression.getField();
        final Evaluator keyEvaluator = fieldExpression.getKey().getEvaluator();
        if (fieldExpression.getSubKey() == null) {
            return record -> {
                TypedObject key = keyEvaluator.evaluate(record);
                if (key.isNull()) {
                    return TypedObject.NULL;
                }
                Type type = key.getType();
                if (Type.isNumeric(type)) {
                    return record.typedGet(field, ((Number) key.getValue()).intValue());
                } else {
                    return record.typedGet(field, (String) key.getValue());
                }
            };
        } else {
            final Evaluator subKeyEvaluator = fieldExpression.getSubKey().getEvaluator();
            return record -> {
                TypedObject key = keyEvaluator.evaluate(record);
                if (key.isNull()) {
                    return TypedObject.NULL;
                }
                TypedObject subKey = subKeyEvaluator.evaluate(record);
                if (subKey.isNull()) {
                    return TypedObject.NULL;
                }
                Type type = key.getType();
                if (Type.isNumeric(type)) {
                    return record.typedGet(field, ((Number) key.getValue()).intValue(), (String) subKey.getValue());
                } else {
                    return record.typedGet(field, (String) key.getValue(), (String) subKey.getValue());
                }
            };
        }
    }
}
