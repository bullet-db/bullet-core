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
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import static com.yahoo.bullet.common.BulletError.makeError;

@Getter
public class OrderBy extends PostAggregation {
    private static final long serialVersionUID = -58085305662163506L;

    public enum Direction {
        ASC,
        DESC
    }

    @Getter
    public static class SortItem implements Serializable {
        private static final long serialVersionUID = 4024279669854156179L;

        private String field;
        private Direction direction;

        public SortItem(String field, Direction direction) {
            this.field = Objects.requireNonNull(field);
            this.direction = Objects.requireNonNull(direction);
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
        Utilities.requireNonNullList(fields);
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("List empty bad");
        }
        this.fields = fields;
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
