/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.postaggregations;

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
     * Constructor that creates an OrderBy post-strategy.
     *
     * @param orderBy The OrderBy post-aggregation to create a strategy for.
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
        return orderBy.getFields().stream().map(OrderByStrategy::fieldComparator).reduce(Comparator::thenComparing).get();
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
