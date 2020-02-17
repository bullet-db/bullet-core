/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.FieldExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

/**
 * Evaluator that extracts the given field from a BulletRecord and casts the result. The is the only evaluator
 * that directly takes a BulletRecord.
 */
public class FieldEvaluator extends Evaluator {
    @FunctionalInterface
    public interface FieldExtractor {
        TypedObject extract(BulletRecord record);
    }

    private FieldExtractor fieldExtractor;

    public FieldEvaluator(FieldExpression fieldExpression) {
        super(fieldExpression);
        fieldExtractor = getFieldExtractor(fieldExpression);
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return new TypedObject(fieldExtractor.extract(record));
    }

    private static FieldExtractor getFieldExtractor(FieldExpression fieldExpression) {
        final String field = fieldExpression.getField();
        final Integer index = fieldExpression.getIndex();
        final String key = fieldExpression.getKey();
        final String subKey = fieldExpression.getSubKey();
        if (index != null) {
            if (subKey != null) {
                return record -> record.typedGet(field, index, subKey);
                //return record -> new TypedObject(record.get(field, index, subKey));
            }
            return record -> record.typedGet(field, index);
            //return record -> new TypedObject(record.get(field, index));
        }
        if (key != null) {
            if (subKey != null) {
                return record -> record.typedGet(field, key, subKey);
                //return record -> new TypedObject(record.get(field, key, subKey));
            }
            return record -> record.typedGet(field, key);
            //return record -> new TypedObject(record.get(field, key));
        }
        return record -> record.typedGet(field);
        //return record -> new TypedObject(record.get(field));
    }
}
