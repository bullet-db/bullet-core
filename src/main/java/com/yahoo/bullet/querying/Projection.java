/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.query.Field;
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
     * Constructor that creates a Projection from the given fields.
     *
     * @param fields The fields to create a Projection from. Will be non-null.
     */
    public Projection(List<Field> fields) {
        evaluators = fields.stream().collect(Collectors.toMap(Field::getName, Projection::getEvaluator));
    }

    /**
     * Projects onto a new BulletRecord.
     *
     * @param record The record to compute new fields from.
     * @param provider The provider to get the new BulletRecord.
     * @return A new BulletRecord with fields projected onto it.
     */
    public BulletRecord project(BulletRecord record, BulletRecordProvider provider) {
        BulletRecord projected = provider.getInstance();
        evaluators.forEach((name, evaluator) -> {
            try {
                TypedObject value = evaluator.evaluate(record);
                if (!value.isNull()) {
                    projected.typedSet(name, value);
                }
            } catch (Exception ignored) {
            }
        });
        return projected;
    }

    /**
     * Projects onto a BulletRecord. Projected fields are projected all at once.
     *
     * @param record The record to compute new fields from and to project onto.
     * @return The original BulletRecord with new fields projected onto it.
     */
    public BulletRecord project(BulletRecord record) {
        Map<String, TypedObject> map = new HashMap<>();
        evaluators.forEach((name, evaluator) -> {
            try {
                TypedObject value = evaluator.evaluate(record);
                if (!value.isNull()) {
                    map.put(name, value);
                }
            } catch (Exception ignored) {
            }
        });
        map.forEach(record::typedSet);
        return record;
    }

    private static Evaluator getEvaluator(Field field) {
        return field.getValue().getEvaluator();
    }
}
