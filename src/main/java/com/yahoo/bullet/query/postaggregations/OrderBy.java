/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.common.Utilities;
import com.yahoo.bullet.querying.postaggregations.OrderByStrategy;
import com.yahoo.bullet.querying.postaggregations.PostStrategy;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

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

        /**
         * Constructor that creates an {@link OrderBy} item.
         *
         * @param field The non-null field to sort by.
         * @param direction The non-null direction to sort by.
         */
        public SortItem(String field, Direction direction) {
            this.field = Objects.requireNonNull(field);
            this.direction = Objects.requireNonNull(direction);
        }

        @Override
        public String toString() {
            return "{field: " + field + ", direction: " + direction + "}";
        }
    }

    public static final BulletException ORDER_BY_REQUIRES_FIELDS =
            new BulletException("The ORDER BY post-aggregation requires at least one field.", "Please add at least one field.");

    private List<SortItem> fields;

    /**
     * Constructor that creates an OrderBy post-aggregation.
     *
     * @param fields The non-null list of fields to order by.
     */
    public OrderBy(List<SortItem> fields) {
        super(PostAggregationType.ORDER_BY);
        Utilities.requireNonNull(fields);
        if (fields.isEmpty()) {
            throw ORDER_BY_REQUIRES_FIELDS;
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
