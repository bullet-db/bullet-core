/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.parsing.Field;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    /**
     *
     * @param fields Non-null required by initialize.
     */
    public Projection(List<Field> fields) {
        evaluators = fields.stream().collect(Collectors.toMap(Field::getName, Projection::getEvaluator));
    }

    public BulletRecord project(BulletRecord record, BulletRecordProvider provider) {
        BulletRecord projected = provider.getInstance();
        evaluators.forEach((name, evaluator) -> {
            try {
                TypedObject value = evaluator.evaluate(record);
                if (value != null && value.getValue() != null) {
                    // ^ NULL check instead?
                    projected.typedSet(name, value);
                    //projected.forceSet(name, value.getValue());
                }
            } catch (Exception ignored) {
            }
        });
        return projected;
    }

    // Used for computation
    public BulletRecord project(BulletRecord record) {
        Map<String, TypedObject> map = new HashMap<>();
        evaluators.forEach((name, evaluator) -> {
            try {
                TypedObject value = evaluator.evaluate(record);
                if (value != null && value.getValue() != null) {
                    map.put(name, value);
                    //map.put(name, value.getValue());
                }
            } catch (Exception ignored) {
            }
        });
        map.forEach(record::typedSet);
        //map.forEach(record::forceSet);
        return record;
    }

    public BulletRecord copyAndProject(BulletRecord record, BulletRecordProvider provider) {
        return project(copy(record, provider.getInstance()));
    }

    private <T> BulletRecord copy(BulletRecord<T> record, BulletRecord<T> projected) {
        record.iterator().forEachRemaining(entry -> projected.typedSet(entry.getKey(), new TypedObject(entry.getValue())));
        //record.iterator().forEachRemaining(entry -> projected.forceSet(entry.getKey(), entry.getValue()));
        return projected;
    }

    private static Evaluator getEvaluator(Field field) {
        return field.getValue().getEvaluator();
    }
}
