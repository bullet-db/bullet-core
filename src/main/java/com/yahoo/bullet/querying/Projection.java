/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.Getter;

import java.util.LinkedHashMap;

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
    private LinkedHashMap<String, Evaluator> evaluators;

    public Projection(com.yahoo.bullet.parsing.Projection projection) {
        if (projection.getFields() == null) {
            return;
        }
        evaluators = new LinkedHashMap<>();
        projection.getFields().forEach(field -> evaluators.put(field.getName(), Evaluator.build(field.getValue())));
    }

    public BulletRecord project(BulletRecord record) {
        if (evaluators == null) {
            return record;
        }
        return project(record, record);
    }

    public BulletRecord project(BulletRecord record, BulletRecordProvider provider) {
        if (evaluators == null) {
            return copy(record, provider.getInstance());
        }
        return project(record, provider.getInstance());
    }

    private BulletRecord project(BulletRecord record, BulletRecord projected) {
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

    private BulletRecord copy(BulletRecord record, BulletRecord projected) {
        record.iterator().forEachRemaining(entry -> projected.forceSet(entry.getKey(), entry.getValue()));
        return projected;
    }
}
