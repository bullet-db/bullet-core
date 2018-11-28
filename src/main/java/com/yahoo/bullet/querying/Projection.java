package com.yahoo.bullet.querying;

import com.yahoo.bullet.parsing.expressions.LazyExpression;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.HashMap;
import java.util.Map;

public class Projection {
    private Map<String, Evaluator> evaluators;

    public Projection(Map<String, LazyExpression> projection) {
        if (projection != null && !projection.isEmpty()) {
            evaluators = new HashMap<>();
            projection.forEach((key, value) -> evaluators.put(key, Evaluator.build(value)));
        }
    }

    public BulletRecord project(BulletRecord record, BulletRecordProvider provider) {
        if (evaluators == null) {
            return record;
        }
        BulletRecord projected = provider.getInstance();
        evaluators.forEach((name, evaluator) -> {
            TypedObject value = evaluator.evaluate(record);
            if (value != null && value.getValue() != null) {
                projected.forceSet(name, value.getValue());
            }
        });
        return projected;
    }
}
