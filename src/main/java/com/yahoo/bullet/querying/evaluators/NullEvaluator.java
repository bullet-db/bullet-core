package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.LazyNull;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

public class NullEvaluator extends Evaluator {
    public NullEvaluator(LazyNull lazyNull) {
        super(lazyNull);
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return new TypedObject(Type.NULL, null);
    }
}
