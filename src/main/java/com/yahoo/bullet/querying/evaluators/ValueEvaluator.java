package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.LazyValue;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

/**
 * Not so lazy.
 */
public class ValueEvaluator extends Evaluator {
    private TypedObject value;

    public ValueEvaluator(LazyValue lazyValue) {
        super(lazyValue);
        this.value = TypedObject.forceCast(type, lazyValue.getValue());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return value;
    }
}
