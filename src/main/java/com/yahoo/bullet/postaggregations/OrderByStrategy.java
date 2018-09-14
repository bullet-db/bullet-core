/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.parsing.OrderBy;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.yahoo.bullet.common.Utilities.extractTypedObject;

@Slf4j
public class OrderByStrategy implements PostStrategy {
    private OrderBy postAggregation;
    private int multiplyingFactor;

    /**
     * Contructor takes a {@link OrderBy} object.
     *
     * @param postAggregation The {@link OrderBy} object.
     */
    public OrderByStrategy(OrderBy postAggregation) {
        this.postAggregation = postAggregation;
        multiplyingFactor = postAggregation.getDirection() == OrderBy.Direction.ASC ? 1 : -1;
    }

    @Override
    public Clip execute(Clip clip) {
        List<BulletRecord> records = clip.getRecords();
        records.sort((a, b) -> {
                for (String field : postAggregation.getFields()) {
                    TypedObject typedObjectA = extractTypedObject(field, a);
                    TypedObject typedObjectB = extractTypedObject(field, b);
                    try {
                        int compareValue = typedObjectA.compareTo(typedObjectB);
                        if (compareValue != 0) {
                            return multiplyingFactor * compareValue;
                        }
                    } catch (RuntimeException e) {
                        // Ignore the exception and skip this field.
                        log.error("Unable to compare field " + field);
                        log.error("Skip it due to: " + e);
                    }
                }
                return 0;
            });
        return clip;
    }
}
