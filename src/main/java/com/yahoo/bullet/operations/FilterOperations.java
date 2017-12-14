/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.operations;

import com.google.gson.annotations.SerializedName;
import com.yahoo.bullet.typesystem.TypedObject;
import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.record.BulletRecord;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.yahoo.bullet.typesystem.TypedObject.IS_NOT_NULL;
import static com.yahoo.bullet.typesystem.TypedObject.IS_NOT_UNKNOWN;
import static java.util.Arrays.asList;

public class FilterOperations {

    public enum FilterType {
        @SerializedName("==")
        EQUALS,
        @SerializedName("!=")
        NOT_EQUALS,
        @SerializedName(">")
        GREATER_THAN,
        @SerializedName("<")
        LESS_THAN,
        @SerializedName(">=")
        GREATER_EQUALS,
        @SerializedName("<=")
        LESS_EQUALS,
        @SerializedName("RLIKE")
        REGEX_LIKE,
        @SerializedName("AND")
        AND,
        @SerializedName("OR")
        OR,
        @SerializedName("NOT")
        NOT;

        public static final List<String> LOGICALS = asList("AND", "OR", "NOT");
        public static final List<String> RELATIONALS = asList("==", "!=", ">=", "<=", ">", "<", "RLIKE");
    }

    @FunctionalInterface
    public interface Comparator<T> {
        /**
         * Performs the comparison operation on the {@link TypedObject} against a {@link List} of values.
         *
         * @param object The {@link TypedObject} that is the subject of the operation.
         * @param values The {@link List} of values that this operation is going to performed with.
         * @return Boolean denoting whether this comparison operation was a success or not.
         */
        boolean compare(TypedObject object, List<T> values);
    }

    // Just to avoid typing BiPredicate<...>
    public interface LogicalOperator extends BiPredicate<BulletRecord, List<Clause>> {
    }

    private static Stream<TypedObject> safeCast(TypedObject object, List<String> values) {
        return values.stream().filter(Objects::nonNull).map(object::typeCast).filter(IS_NOT_UNKNOWN);
    }

    // These predicates WILL satisfy the "vacuous" truth checks. That is if the stream is empty, allMatch and
    // noneMatch will return true; anyMatch will return false. This means that if after failing to cast all values
    // to t's type causing the stream to be empty, the any/all/none matches will behave as above.
    // For example:
    // SOME_LONG_VALUE EQ [1.23, 35.2] will be false
    // SOME_LONG_VALUE NE [1.23. 425.3] will be false
    // SOME_LONG_VALUE GT/LT/GE/LE [12.4, 253.4] will be false! even if SOME_LONG_VALUE numerically could make it true.
    public static final Comparator<String> EQ = (t, v) -> safeCast(t, v).anyMatch(i -> t.compareTo(i) == 0);
    public static final Comparator<String> NE = (t, v) -> safeCast(t, v).noneMatch(i -> t.compareTo(i) == 0);
    public static final Comparator<String> GT = (t, v) -> IS_NOT_NULL.test(t) && safeCast(t, v).anyMatch(i -> t.compareTo(i) > 0);
    public static final Comparator<String> LT = (t, v) -> IS_NOT_NULL.test(t) && safeCast(t, v).anyMatch(i -> t.compareTo(i) < 0);
    public static final Comparator<String> GE = (t, v) -> IS_NOT_NULL.test(t) && safeCast(t, v).anyMatch(i -> t.compareTo(i) >= 0);
    public static final Comparator<String> LE = (t, v) -> IS_NOT_NULL.test(t) && safeCast(t, v).anyMatch(i -> t.compareTo(i) <= 0);
    public static final Comparator<Pattern> RLIKE = (t, v) -> IS_NOT_NULL.test(t) &&
                                                              v.stream().map(p -> p.matcher(t.toString()))
                                                               .anyMatch(Matcher::matches);

    public static final LogicalOperator AND = (r, l) -> l.stream().map(c -> c.check(r)).allMatch(Boolean::valueOf);
    public static final LogicalOperator  OR = (r, l) -> l.stream().map(c -> c.check(r)).anyMatch(Boolean::valueOf);
    public static final LogicalOperator NOT = (r, l) -> !l.get(0).check(r);

    // Convenience maps from operators to operations, grouped by type.
    public static final Map<FilterType, Comparator> RELATIONAL_OPERATORS = new EnumMap<>(FilterType.class);
    static {
        RELATIONAL_OPERATORS.put(FilterType.EQUALS, EQ);
        RELATIONAL_OPERATORS.put(FilterType.NOT_EQUALS, NE);
        RELATIONAL_OPERATORS.put(FilterType.GREATER_THAN, GT);
        RELATIONAL_OPERATORS.put(FilterType.LESS_THAN, LT);
        RELATIONAL_OPERATORS.put(FilterType.GREATER_EQUALS, GE);
        RELATIONAL_OPERATORS.put(FilterType.LESS_EQUALS, LE);
        RELATIONAL_OPERATORS.put(FilterType.REGEX_LIKE, RLIKE);
    }

    public static final Map<FilterType, LogicalOperator> LOGICAL_OPERATORS = new EnumMap<>(FilterType.class);
    static {
        LOGICAL_OPERATORS.put(FilterType.AND, AND);
        LOGICAL_OPERATORS.put(FilterType.OR, OR);
        LOGICAL_OPERATORS.put(FilterType.NOT, NOT);
    }
}
