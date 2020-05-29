/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.aggregations.grouping;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import static java.util.Arrays.asList;

/**
 * This class captures an operation that will be performed on an entire group - counts, sums, mins etc.
 * Other than count, all other operations include a field name on which the operation is applied.
 */
@Getter
public class GroupOperation implements Serializable {
    private static final long serialVersionUID = 40039294765462402L;

    @Getter @AllArgsConstructor
    public enum GroupOperationType {
        COUNT("COUNT"),
        SUM("SUM"),
        MIN("MIN"),
        MAX("MAX"),
        AVG("AVG"),
        // COUNT_FIELD operation is only used internally in conjunction with AVG and won't be returned.
        COUNT_FIELD("COUNT_FIELD");

        private String name;
    }

    public interface GroupOperator extends BiFunction<Number, Number, Number> {
    }

    // If either argument is null, a NullPointerException will be thrown.
    public static final GroupOperator MIN = (x, y) -> x.doubleValue() <  y.doubleValue() ? x : y;
    public static final GroupOperator MAX = (x, y) -> x.doubleValue() >  y.doubleValue() ? x : y;
    public static final GroupOperator SUM = (x, y) -> x.doubleValue() + y.doubleValue();
    public static final GroupOperator COUNT = (x, y) -> x.longValue() + y.longValue();

    public static final Map<GroupOperationType, GroupOperator> OPERATORS = new EnumMap<>(GroupOperationType.class);
    static {
        OPERATORS.put(GroupOperationType.COUNT, GroupOperation.COUNT);
        OPERATORS.put(GroupOperationType.COUNT_FIELD, GroupOperation.COUNT);
        OPERATORS.put(GroupOperationType.SUM, GroupOperation.SUM);
        OPERATORS.put(GroupOperationType.MIN, GroupOperation.MIN);
        OPERATORS.put(GroupOperationType.MAX, GroupOperation.MAX);
        OPERATORS.put(GroupOperationType.AVG, GroupOperation.SUM);
    }

    public static final Set<GroupOperationType> SUPPORTED_GROUP_OPERATIONS =
            new HashSet<>(asList(GroupOperationType.COUNT, GroupOperationType.AVG, GroupOperationType.MAX,
                                 GroupOperationType.MIN, GroupOperationType.SUM));

    private final GroupOperationType type;
    private final String field;
    // Ignored purposefully for hashCode and equals
    private final String name;

    /**
     * Constructor that creates a group operation.
     *
     * @param type The non-null type of the group operation.
     * @param field The field of the group operation. Can be null for COUNT operation.
     * @param name The name of the group operation. Can be null for COUNT_FIELD operation.
     */
    public GroupOperation(GroupOperationType type, String field, String name) {
        switch (type) {
            case COUNT:
                this.field = null;
                this.name = Objects.requireNonNull(name);
                break;
            case SUM:
            case MIN:
            case MAX:
            case AVG:
                this.field = Objects.requireNonNull(field);
                this.name = Objects.requireNonNull(name);
                break;
            default:
                // COUNT_FIELD
                this.field = Objects.requireNonNull(field);
                this.name = null;
                break;
        }
        this.type = type;
    }

    @Override
    public int hashCode() {
        // Not relying on Enum hashcode
        String typeString = type == null ? null : type.getName();
        return Objects.hash(typeString, field);
    }

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof GroupOperation)) {
            return false;
        }
        GroupOperation other = (GroupOperation) object;
        if (type != other.type) {
            return false;
        }
        if (field == null && other.field == null) {
            return true;
        }
        return field != null && field.equals(other.field);
    }

    @Override
    public String toString() {
        return "{type: " + type + ", field: " + field + ", name: " + name + "}";
    }
}
