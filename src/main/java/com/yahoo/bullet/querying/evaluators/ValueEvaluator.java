package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.LazyValue;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

public class ValueEvaluator extends Evaluator {
    private String value;

    public ValueEvaluator(LazyValue lazyValue) {
        super(lazyValue);
        this.value = lazyValue.getValue();
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        // placeholder
        try {
            return TypedObject.forceCast(type, value);
        } catch (Exception e) {
            return null;
        }
    }
}
