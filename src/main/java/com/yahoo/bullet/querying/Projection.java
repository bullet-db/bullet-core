/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.record.BulletRecordProvider;
import com.yahoo.bullet.typesystem.TypedObject;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Projection consists of a mapping of names to evaluators built from the projection in the Bullet query.
 *
 * If an evaluator fails, only the corresponding field will not be projected, i.e. an evaluator failing does not fail
 * the entire projection. If all evaluators fail, there will be an empty record.
 *
 * Nulls are not projected.
 */
public class Projection {
    private final Map<String, Evaluator> evaluators;
    private Evaluator explodeEvaluator;
    private String keyAlias;
    private String valueAlias;
    private boolean outerLateralView = false;

    /**
     * Constructor that creates a Projection from the given fields.
     *
     * @param fields The non-null fields to create a Projection from.
     */
    public Projection(List<Field> fields) {
        evaluators = fields.stream().collect(Collectors.toMap(Field::getName, Projection::getEvaluator));
    }

    /**
     * Constructor that creates a Projection from the given {@link com.yahoo.bullet.query.Projection}.
     *
     * @param projection The non-null projection to create a Projection from.
     */
    public Projection(com.yahoo.bullet.query.Projection projection) {
        this(projection.getFields());
        com.yahoo.bullet.query.Projection.Explode explode = projection.getExplode();
        if (explode != null) {
            explodeEvaluator = explode.getField().getEvaluator();
            keyAlias = explode.getKeyAlias();
            valueAlias = explode.getValueAlias();
            outerLateralView = explode.isOuterLateralView();
        }
    }

    /**
     * Projects onto a new BulletRecord.
     *
     * @param record The record to compute new fields from.
     * @param provider The provider to get the new BulletRecord.
     * @return A new BulletRecord with fields projected onto it.
     */
    public BulletRecord project(BulletRecord record, BulletRecordProvider provider) {
        return projectRecord(record, provider::getInstance);
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

    /**
     *
     *
     * @param record
     * @param provider
     * @return
     */
    public List<BulletRecord> explode(BulletRecord record, BulletRecordProvider provider) {
        TypedObject explodeField;
        try {
            explodeField = explodeEvaluator.evaluate(record);
        } catch (Exception e) {
            explodeField = TypedObject.NULL;
        }
        if (valueAlias != null) {
            return explodeMap(explodeField, record, provider::getInstance);
        } else {
            return explodeList(explodeField, record, provider::getInstance);
        }
    }

    /**
     *
     * @param record
     * @return
     */
    public List<BulletRecord> explode(BulletRecord record) {
        TypedObject explodeField;
        try {
            explodeField = explodeEvaluator.evaluate(record);
        } catch (Exception e) {
            explodeField = TypedObject.NULL;
        }
        if (valueAlias != null) {
            return explodeMap(explodeField, record, record::copy);
        } else {
            return explodeList(explodeField, record, record::copy);
        }
    }

    private List<BulletRecord> explodeList(TypedObject explodeField, BulletRecord record, Supplier<BulletRecord> supplier) {
        List<Serializable> explodedList;
        if (!explodeField.isList() || explodeField.size() == 0) {
            if (!outerLateralView) {
                return Collections.emptyList();
            }
            explodedList = Collections.singletonList(null);
        } else {
            explodedList = (List<Serializable>) explodeField.getValue();
        }
        BulletRecordWrapper wrapper = new BulletRecordWrapper(record);
        return explodedList.stream().map(s -> {
            wrapper.put(keyAlias, s);
            // TODO: Inefficient for evaluators that only depend on the base record
            return projectRecord(wrapper, supplier);
        }).collect(Collectors.toList());
    }

    private List<BulletRecord> explodeMap(TypedObject explodeField, BulletRecord record, Supplier<BulletRecord> supplier) {
        Map<String, Serializable> explodedMap;
        if (!explodeField.isList() || explodeField.size() == 0) {
            if (!outerLateralView) {
                return Collections.emptyList();
            }
            explodedMap = Collections.singletonMap(null, null);
        } else {
            explodedMap = (Map<String, Serializable>) explodeField.getValue();
        }
        BulletRecordWrapper wrapper = new BulletRecordWrapper(record);
        return explodedMap.entrySet().stream().map(entry -> {
            wrapper.put(keyAlias, entry.getKey());
            wrapper.put(valueAlias, entry.getValue());
            // TODO: Inefficient for evaluators that only depend on the base record
            return projectRecord(wrapper, supplier);
        }).collect(Collectors.toList());
    }

    private BulletRecord projectRecord(BulletRecord record, Supplier<BulletRecord> supplier) {
        BulletRecord projected = supplier.get();
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

    private static Evaluator getEvaluator(Field field) {
        return field.getValue().getEvaluator();
    }
}
