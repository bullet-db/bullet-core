/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.OrderBy;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.Utilities.extractTypedObject;

@AllArgsConstructor
public class OrderByStrategy implements PostStrategy {
    private OrderBy postAggregation;

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
                            return postAggregation.getDirection() == OrderBy.Direction.ASC ? compareValue : -1 * compareValue;
                        }
                    } catch (RuntimeException e) {
                        // Ignore the exception and skip this field.
                    }
                }
                return 0;
            });
        return clip;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        return Optional.empty();
    }
}