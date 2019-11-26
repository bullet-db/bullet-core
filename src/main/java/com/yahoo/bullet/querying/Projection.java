package com.yahoo.bullet.querying;

import com.yahoo.bullet.parsing.Field;
import com.yahoo.bullet.parsing.expressions.FieldExpression;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.querying.evaluators.FieldEvaluator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Projection consists of a map of names to evaluators built from the projection map in the bullet query. If there's no projection,
 * the entire record is returned.
 *
 * Null values will not be projected.
 *
 * Also, if an evaluator fails, only the corresponding field will not be projected, i.e. an evaluator failing won't fail
 * the entire projection. (Not sure if this  behavior is expected/wanted)
 *
 * For now, if all evaluators fail, it is possible to have an empty record.
 */
@Getter
public class Projection {
    private Map<String, Evaluator> evaluators;
    private boolean inclusive;

    public Projection(List<Field> fields) {
        if (fields != null && !fields.isEmpty()) {
            evaluators = new LinkedHashMap<>();
            fields.forEach(field -> evaluators.put(field.getName(), Evaluator.build(field.getValue())));
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

    public void addTransientFields(Set<String> transientFields) {
        for (String field : transientFields) {
            evaluators.put(field, new FieldEvaluator(new FieldExpression(field)));
        }
    }
}
