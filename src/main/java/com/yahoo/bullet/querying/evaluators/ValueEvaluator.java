package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.LazyValue;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

/**
 * Not so lazy. Need to make sure TypedObjects are properly immutable.
 */
public class ValueEvaluator extends Evaluator {
    private final TypedObject value;

    public ValueEvaluator(LazyValue lazyValue) {
        super(lazyValue);
        this.value = TypedObject.forceCast(type, lazyValue.getValue());
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return value;
    }
}
