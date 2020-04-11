/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.postaggregations.OrderByStrategy;
import com.yahoo.bullet.postaggregations.PostStrategy;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter
public class OrderBy extends PostAggregation {
    private static final long serialVersionUID = -58085305662163506L;

    public enum Direction {
        ASC,
        DESC
    }

    @Getter @Setter @AllArgsConstructor
    public static class SortItem {
        private String field;
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

    private List<SortItem> fields;

    public static final BulletError ORDERBY_REQUIRES_FIELDS_ERROR =
            makeError("The ORDERBY post aggregation needs at least one field", "Please add fields.");
    public static final BulletError ORDERBY_REQUIRES_NON_EMPTY_FIELDS_ERROR =
            makeError("The fields in ORDERBY post aggregation must not be empty", "Please add non-empty fields.");

    public OrderBy(List<SortItem> fields) {
        super(Type.ORDER_BY);
        this.fields = Utilities.requireNonNullList(fields);
    }

    @Override
    public PostStrategy getPostStrategy() {
        return new OrderByStrategy(this);
    }

    @Override
    public String toString() {
        return "{type: " + type + ", fields: " + fields + "}";
    }
}
