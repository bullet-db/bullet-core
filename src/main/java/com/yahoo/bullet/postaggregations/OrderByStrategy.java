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
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j @AllArgsConstructor
public class OrderByStrategy implements PostStrategy {
    private OrderBy orderBy;

    @Override
    public Clip execute(Clip clip) {
        List<BulletRecord> records = clip.getRecords();
        records.sort((a, b) -> {
            for (OrderBy.SortItem sortItem : orderBy.getFields()) {
                TypedObject typedObjectA = a.typedGet(sortItem.getField());
                TypedObject typedObjectB = b.typedGet(sortItem.getField());
                try {
                    int compareValue;
                    if (typedObjectA.isNull()) {
                        compareValue = typedObjectB.isNull() ? 0 : -1;
                    } else if (typedObjectB.isNull()) {
                        compareValue = 1;
                    } else {
                        compareValue = typedObjectA.compareTo(typedObjectB);
                    }
                    if (compareValue != 0) {
                        return (sortItem.getDirection() == OrderBy.Direction.ASC ? 1 : -1) * compareValue;
                    }
                } catch (RuntimeException e) {
                    // Ignore the exception and skip this field.
                    log.error("Unable to compare field " + sortItem.getField());
                    log.error("Skip it due to: " + e);
                }
            }
            return 0;
        });
        return clip;
    }
}
