/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.google.gson.annotations.Expose;
import com.yahoo.bullet.common.BulletError;
import lombok.AllArgsConstructor;
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

    @Getter @AllArgsConstructor
    public static class SortItem {
        @Expose
        private String field;
        @Expose
        private Direction direction;

        /**
         * Default constructor. GSON recommended
         */
        public SortItem() {
            field = null;
            direction = Direction.ASC;
        }

        @Override
        public String toString() {
            return "{field: " + field + ", direction: " + direction + "}";
        }
    }

    @Expose
    private List<SortItem> sortItems;

    public static final BulletError ORDERBY_REQUIRES_FIELDS_ERROR =
            makeError("The ORDERBY post aggregation needs at least one sort item", "Please add sort items.");
    public static final BulletError ORDERBY_REQUIRES_NON_EMPTY_FIELDS_ERROR =
            makeError("The fields in ORDERBY post aggregation must not be empty", "Please add non-empty fields.");

    /**
     * Default constructor. GSON recommended
     */
    public OrderBy() {
        sortItems = null;
    }

    @Override
    public String toString() {
        return "{type: " + type + ", sortItems: " + sortItems + "}";
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        Optional<List<BulletError>> errors = super.initialize();
        if (errors.isPresent()) {
            return errors;
        }
        if (sortItems == null || sortItems.isEmpty()) {
            return Optional.of(Collections.singletonList(ORDERBY_REQUIRES_FIELDS_ERROR));
        }
        if (sortItems.stream().anyMatch(sortItem -> sortItem.getField() == null || sortItem.getField().isEmpty())) {
            return Optional.of(Collections.singletonList(ORDERBY_REQUIRES_NON_EMPTY_FIELDS_ERROR));
        }
        return Optional.empty();
    }

}
