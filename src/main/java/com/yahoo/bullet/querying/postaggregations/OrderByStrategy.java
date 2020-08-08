/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.postaggregations;

import com.yahoo.bullet.query.expressions.Expression;
import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.querying.evaluators.Evaluator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class OrderByStrategy implements PostStrategy {
    private static final Comparator<TypedObject> NULLS_FIRST = TypedObject.nullsFirst();

    private final Comparator<BulletRecord> comparator;
    private final List<Evaluator> evaluators;
    private final List<OrderBy.Direction> directions;
    private final Map<BulletRecord, LazyArray> mapping;
    private final int numFields;

    class LazyArray {
        private final BulletRecord record;
        private final TypedObject[] values;

        LazyArray(BulletRecord record, int capacity) {
            this.record = record;
            this.values = new TypedObject[capacity];
        }

        TypedObject get(int index) {
            TypedObject value = values[index];
            if (value == null) {
                try {
                    value = evaluators.get(index).evaluate(record);
                } catch (Exception e) {
                    value = TypedObject.NULL;
                }
                values[index] = value;
            }
            return value;
        }
    }

    /**
     * Constructor that creates an OrderBy post-strategy.
     *
     * @param orderBy The OrderBy post-aggregation to create a strategy for.
     */
    public OrderByStrategy(OrderBy orderBy) {
        evaluators = orderBy.getFields().stream().map(OrderBy.SortItem::getExpression).map(Expression::getEvaluator).collect(Collectors.toList());
        directions = orderBy.getFields().stream().map(OrderBy.SortItem::getDirection).collect(Collectors.toList());
        mapping = new HashMap<>();
        numFields = orderBy.getFields().size();
        comparator = getComparator();
    }

    @Override
    public Clip execute(Clip clip) {
        List<BulletRecord> records = clip.getRecords();
        records.forEach(record -> mapping.put(record, new LazyArray(record, numFields)));
        records.sort(comparator);
        mapping.clear();
        return clip;
    }

    private Comparator<BulletRecord> getComparator() {
        return (a, b) -> {
            LazyArray lazyArrayA = mapping.get(a);
            LazyArray lazyArrayB = mapping.get(b);
            int c;
            for (int i = 0; i < numFields; i++) {
                c = directions.get(i) == OrderBy.Direction.ASC ? NULLS_FIRST.compare(lazyArrayA.get(i), lazyArrayB.get(i))
                                                               : NULLS_FIRST.compare(lazyArrayB.get(i), lazyArrayA.get(i));
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        };
    }
}
