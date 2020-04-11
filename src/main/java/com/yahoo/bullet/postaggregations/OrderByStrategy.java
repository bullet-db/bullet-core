/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.query.postaggregations.OrderBy;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;

@Slf4j
public class OrderByStrategy implements PostStrategy {
    private final Comparator<BulletRecord> comparator;

    /**
     * Constructor for OrderBy strategy.
     *
     * @param orderBy OrderBy post aggregation.
     */
    public OrderByStrategy(OrderBy orderBy) {
        comparator = getComparator(orderBy);
    }

    @Override
    public Clip execute(Clip clip) {
        List<BulletRecord> records = clip.getRecords();
        records.sort(comparator);
        return clip;
    }

    private static Comparator<BulletRecord> getComparator(OrderBy orderBy) {
        /*
        List<OrderBy.SortItem> fields = orderBy.getFields();
        Comparator<BulletRecord> comparator = fieldComparator(fields.get(0));
        int size = fields.size();
        for (int i = 1; i < size; i++) {
            comparator = comparator.thenComparing(fieldComparator(fields.get(i)));
        }
        return comparator;
        */
        // TODO Is above or below preferred? below is cleaner but has a repeated (but trivial) null check
        // TODO Could also do the thenComparing backwards ..
        Comparator<BulletRecord> comparator = null;
        for (OrderBy.SortItem sortItem : orderBy.getFields()) {
            if (comparator == null) {
                comparator = fieldComparator(sortItem);
            } else {
                comparator = comparator.thenComparing(fieldComparator(sortItem));
            }
        }
        return comparator;
    }

    private static Comparator<BulletRecord> fieldComparator(OrderBy.SortItem sortItem) {
        return (a, b) -> {
            TypedObject typedObjectA = a.typedGet(sortItem.getField());
            TypedObject typedObjectB = b.typedGet(sortItem.getField());
            return sortItem.getDirection() == OrderBy.Direction.ASC ? TypedObject.nullsFirst().compare(typedObjectA, typedObjectB)
                                                                    : TypedObject.nullsFirst().compare(typedObjectB, typedObjectA);
        };
    }
}
