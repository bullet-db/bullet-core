package com.yahoo.bullet.querying;

import com.yahoo.bullet.parsing.expressions.Expression;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Projection consists of a map of names to evaluators built from the projection map in the bullet query. If there's no projection,
 * the entire record is returned.
 *
 * Null values will not be projected.
 *
 * Also, if an evaluator fails, only the corresponding field will not be projected, i.e. an evaluator failing won't fail
 * the entire projection. (Not sure if this behavior is expected/wanted)
 *
 * For now, if all evaluators fail, it is possible to have an empty record.
 */
public class Projection {
    private Map<String, Evaluator> evaluators;

    public Projection(Map<String, Expression> projection) {
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
                try {
                    TypedObject value = evaluator.evaluate(record);
                    if (value != null && value.getValue() != null) {
                        projected.forceSet(name, value.getValue());
                    }
                } catch (Exception ignored) {
                }
            });
        return projected;
    }
}
