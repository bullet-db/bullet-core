/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import lombok.Getter;
import lombok.Setter;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter @Setter
public class OrderBy extends PostAggregation {
    public enum Direction {
        ASC,
        DESC
    }

    @Expose
    private List<String> fields;
    @Expose
    Direction direction;

    public static final BulletError ORDERBY_REQUIRES_FIELDS_ERROR =
            makeError("The ORDERBY post aggregation needs at least one field", "Please add fields.");

    /**
     * Default constructor. GSON recommended
     */
    public OrderBy() {
        fields = null;
        direction = Direction.ASC;
    }

    @Override
    public String toString() {
        return "{type: " + type + ", fields: " + fields + ", direction: " + direction + "}";
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        Optional<List<BulletError>> errors = super.initialize();
        if (errors.isPresent()) {
            return errors;
        }
        if (fields == null || fields.isEmpty()) {
            return Optional.of(Collections.singletonList(ORDERBY_REQUIRES_FIELDS_ERROR));
        }
        return Optional.empty();
    }

}
