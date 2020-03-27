package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.CastExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

public class CastEvaluator extends Evaluator {
    private Evaluator value;
    private Type castType;

    public CastEvaluator(CastExpression castExpression) {
        super(castExpression);
        value = castExpression.getValue().getEvaluator();
        castType = castExpression.getCastType();
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        return value.evaluate(record).forceCast(castType);
    }
}
