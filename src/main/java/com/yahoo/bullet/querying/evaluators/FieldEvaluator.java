package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.parsing.expressions.FieldExpression;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

/**
 * Evaluator that extracts the given field from a BulletRecord and casts the result. The is the only evaluator
 * that directly takes a BulletRecord.
 */
public class FieldEvaluator extends Evaluator {
    private String field;

    public FieldEvaluator(FieldExpression fieldExpression) {
        super(fieldExpression);
        this.field = fieldExpression.getField();
    }

    @Override
    public TypedObject evaluate(BulletRecord record) {
        TypedObject object = new TypedObject(record.extractField(field));
        return cast(object);
    }
}
