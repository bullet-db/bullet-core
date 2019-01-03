package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.NullExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

/**
 * Not so lazy. Need to make sure TypedObjects are properly immutable.
 *
 * Not sure if useful though.
 */
public class NullEvaluator extends Evaluator {
    private static final TypedObject TYPED_NULL = new TypedObject(Type.NULL, null);

    public NullEvaluator(NullExpression nullExpression) {
        super(nullExpression);
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return TYPED_NULL;
    }
}
