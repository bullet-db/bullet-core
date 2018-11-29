package com.yahoo.bullet.querying;

import com.yahoo.bullet.parsing.expressions.LazyExpression;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;

public class Filter {
    private Evaluator evaluator;

    public Filter(LazyExpression filter) {
        evaluator = Evaluator.build(filter);
    }

    public boolean match(BulletRecord record) {
        if (evaluator == null) {
            return true;
        }
        try {
            return (Boolean) evaluator.evaluate(record).forceCast(Type.BOOLEAN).getValue();
        } catch (Exception e) {
            return false;
        }
    }
}
