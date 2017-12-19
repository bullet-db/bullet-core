/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.google.gson.annotations.SerializedName;
import com.yahoo.bullet.parsing.FilterClause;
import com.yahoo.bullet.parsing.LogicalClause;
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

import static com.yahoo.bullet.common.Utilities.extractTypedObject;
import static com.yahoo.bullet.common.Utilities.isEmpty;
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
         * Performs the comparison operation on the {@link TypedObject} against a {@link Stream} of values.
         *
         * @param object The {@link TypedObject} that is the subject of the operation.
         * @param values The {@link Stream} of values that this operation is going to performed with.
         * @return Boolean denoting whether this comparison operation was a success or not.
         */
        boolean compare(TypedObject object, Stream<T> values);
    }

    // Avoids typing BiPredicate<...>
    @FunctionalInterface
    public interface LogicalOperator extends BiPredicate<BulletRecord, Stream<Boolean>> {
    }

    // These comparators WILL satisfy the "vacuous" truth checks. That is if the stream is empty, allMatch and
    // noneMatch will return true; anyMatch will return false. This means that if after failing to cast all values
    // to t's type causing the stream to be empty, the any/all/none matches will behave as above.
    // For example:
    // SOME_LONG_VALUE EQ [1.23, 35.2] will be false
    // SOME_LONG_VALUE NE [1.23. 425.3] will be false
    // SOME_LONG_VALUE GT/LT/GE/LE [12.4, 253.4] will be false! even if SOME_LONG_VALUE numerically could make it true.
    private static final Comparator<TypedObject> EQ = (t, s) -> s.anyMatch(i -> t.compareTo(i) == 0);
    private static final Comparator<TypedObject> NE = (t, s) -> s.noneMatch(i -> t.compareTo(i) == 0);
    private static final Comparator<TypedObject> GT = (t, s) -> s.anyMatch(i -> t.compareTo(i) > 0);
    private static final Comparator<TypedObject> LT = (t, s) -> s.anyMatch(i -> t.compareTo(i) < 0);
    private static final Comparator<TypedObject> GE = (t, s) -> s.anyMatch(i -> t.compareTo(i) >= 0);
    private static final Comparator<TypedObject> LE = (t, s) -> s.anyMatch(i -> t.compareTo(i) <= 0);
    private static final Comparator<Pattern> RLIKE = (t, s) -> s.map(p -> p.matcher(t.toString())).anyMatch(Matcher::matches);
    private static final LogicalOperator AND = (r, s) -> s.allMatch(Boolean::valueOf);
    private static final LogicalOperator OR = (r, s) -> s.anyMatch(Boolean::valueOf);
    private static final LogicalOperator NOT = (r, s) -> !s.findFirst().get();

    static final Map<FilterType, Comparator<TypedObject>> COMPARATORS = new EnumMap<>(FilterType.class);
    static {
        COMPARATORS.put(FilterType.EQUALS, EQ);
        COMPARATORS.put(FilterType.NOT_EQUALS, NE);
        COMPARATORS.put(FilterType.GREATER_THAN, isNotNullAnd(GT));
        COMPARATORS.put(FilterType.LESS_THAN, isNotNullAnd(LT));
        COMPARATORS.put(FilterType.GREATER_EQUALS, isNotNullAnd(GE));
        COMPARATORS.put(FilterType.LESS_EQUALS, isNotNullAnd(LE));
    }
    static final Comparator<Pattern> REGEX_LIKE = isNotNullAnd(RLIKE);
    static final Map<FilterType, LogicalOperator> LOGICAL_OPERATORS = new EnumMap<>(FilterType.class);
    static {
        LOGICAL_OPERATORS.put(FilterType.AND, AND);
        LOGICAL_OPERATORS.put(FilterType.OR, OR);
        LOGICAL_OPERATORS.put(FilterType.NOT, NOT);
    }

    private static Stream<TypedObject> cast(TypedObject object, List<String> values) {
        // Right now, we cast the filter values which are lists of strings to the value being filtered on's type.
        // In the future, we might want to support providing non-String values.
        return values.stream().filter(Objects::nonNull).map(object::typeCast).filter(IS_NOT_UNKNOWN);
    }

    private static <T> Comparator<T> isNotNullAnd(Comparator<T> comparator) {
        return (t, s) -> IS_NOT_NULL.test(t) && comparator.compare(t, s);
    }

    private static boolean performRelational(BulletRecord record, FilterClause clause) {
        FilterType operator = clause.getOperation();
        if (isEmpty(clause.getValues())) {
            return true;
        }
        TypedObject object = extractTypedObject(clause.getField(), record);
        if (operator == FilterType.REGEX_LIKE) {
            return REGEX_LIKE.compare(object, clause.getPatterns().stream());
        }
        return COMPARATORS.get(operator).compare(object, cast(object, clause.getValues()));
    }

    private static boolean performLogical(BulletRecord record, LogicalClause clause) {
        List<Clause> clauses = clause.getClauses();
        if (isEmpty(clauses)) {
            return true;
        }
        Stream<Boolean> results = clauses.stream().map(c -> perform(record, c));
        return LOGICAL_OPERATORS.get(clause.getOperation()).test(record, results);
    }

    /**
     * Perform the {@link Clause} on the given {@link BulletRecord} and returns the result.
     *
     * @param record The BulletRecord that is the subject of this clause.
     * @param clause The Clause that is being applied.
     * @return The result of th
     */
    public static boolean perform(BulletRecord record, Clause clause) {
        // Rather than define another hierarchy of Clause -> FilterClause, LogicalClause evaluators, we'll eat the
        // cost of violating polymorphism in this one spot.
        // We do not want processing logic in FilterClause or LogicalClause, otherwise we could put the appropriate
        // methods in those classes.
        return clause instanceof FilterClause ? performRelational(record, (FilterClause) clause)
                                              : performLogical(record, (LogicalClause) clause);
    }
}

