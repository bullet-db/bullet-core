package com.yahoo.bullet.querying;

import com.yahoo.bullet.parsing.expressions.LazyExpression;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.TypedObject;

public class Filter {
    private Evaluator evaluator;

    public Filter(LazyExpression filter) {
        evaluator = Evaluator.build(filter);
    }

    public boolean match(BulletRecord record) {
        if (evaluator == null) {
            return true;
        }
        return (Boolean) evaluator.evaluate(record).getValue();
    }
}
