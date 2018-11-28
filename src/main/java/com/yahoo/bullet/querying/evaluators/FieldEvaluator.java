package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.LazyField;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

public class FieldEvaluator extends Evaluator {
    private String field;

    public FieldEvaluator(LazyField lazyField) {
        super(lazyField);
        this.field = lazyField.getField();
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        Object value = record.extractField(field);
        return value != null ? new TypedObject(value) : null;
    }
}
