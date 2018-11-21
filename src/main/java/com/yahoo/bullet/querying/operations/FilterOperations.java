/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.operations;

import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.LogicalClause;
import com.yahoo.bullet.parsing.ObjectFilterClause;
import com.yahoo.bullet.parsing.Value;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.extern.slf4j.Slf4j;

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
import static com.yahoo.bullet.typesystem.TypedObject.IS_PRIMITIVE_OR_NULL;

@Slf4j
public class FilterOperations {
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
    private static final Comparator<TypedObject> EQ = (t, s) -> s.anyMatch(t::equalTo);
    private static final Comparator<TypedObject> NE = (t, s) -> s.noneMatch(t::equalTo);
    private static final Comparator<TypedObject> GT = (t, s) -> s.anyMatch(i -> t.compareTo(i) > 0);
    private static final Comparator<TypedObject> LT = (t, s) -> s.anyMatch(i -> t.compareTo(i) < 0);
    private static final Comparator<TypedObject> GE = (t, s) -> s.anyMatch(i -> t.compareTo(i) >= 0);
    private static final Comparator<TypedObject> LE = (t, s) -> s.anyMatch(i -> t.compareTo(i) <= 0);
    private static final Comparator<Pattern> RLIKE = (t, s) -> s.map(p -> p.matcher(t.toString())).anyMatch(Matcher::matches);
    private static final Comparator<TypedObject> SIZEIS = (t, s) -> s.anyMatch(i -> i.equalTo(t.size()));
    private static final Comparator<TypedObject> CONTAINSKEY = (t, s) -> s.anyMatch(i -> t.containsKey((String) i.getValue()));
    private static final Comparator<TypedObject> CONTAINSVALUE = (t, s) -> s.anyMatch(t::containsValue);
    private static final LogicalOperator AND = (r, s) -> s.allMatch(Boolean::valueOf);
    private static final LogicalOperator OR = (r, s) -> s.anyMatch(Boolean::valueOf);
    private static final LogicalOperator NOT = (r, s) -> !s.findFirst().get();

    static final Map<Clause.Operation, Comparator<TypedObject>> COMPARATORS = new EnumMap<>(Clause.Operation.class);
    static {
        COMPARATORS.put(Clause.Operation.EQUALS, EQ);
        COMPARATORS.put(Clause.Operation.NOT_EQUALS, NE);
        COMPARATORS.put(Clause.Operation.GREATER_THAN, isNotNullAnd(GT));
        COMPARATORS.put(Clause.Operation.LESS_THAN, isNotNullAnd(LT));
        COMPARATORS.put(Clause.Operation.GREATER_EQUALS, isNotNullAnd(GE));
        COMPARATORS.put(Clause.Operation.LESS_EQUALS, isNotNullAnd(LE));
        COMPARATORS.put(Clause.Operation.SIZE_IS, isNotNullAnd(SIZEIS));
        COMPARATORS.put(Clause.Operation.CONTAINS_KEY, isNotNullAnd(CONTAINSKEY));
        COMPARATORS.put(Clause.Operation.CONTAINS_VALUE, isNotNullAnd(CONTAINSVALUE));
    }
    static final Comparator<Pattern> REGEX_LIKE = isNotNullAnd(RLIKE);
    static final Map<Clause.Operation, LogicalOperator> LOGICAL_OPERATORS = new EnumMap<>(Clause.Operation.class);
    static {
        LOGICAL_OPERATORS.put(Clause.Operation.AND, AND);
        LOGICAL_OPERATORS.put(Clause.Operation.OR, OR);
        LOGICAL_OPERATORS.put(Clause.Operation.NOT, NOT);
    }

    /**
     * Exposed for testing. Cast the values to the type of the object if possible.
     *
     * @param type The {@link Type} to cast the values to.
     * @param values The {@link List} of values to try and cast to the object.
     * @return A {@link Stream} of casted {@link TypedObject}.
     */
    static Stream<TypedObject> cast(BulletRecord record, Type type, List<Value> values) {
        return values.stream().filter(Objects::nonNull).map(v -> getTypedValue(record, type, v)).filter(IS_PRIMITIVE_OR_NULL);
    }

    private static <T> Comparator<T> isNotNullAnd(Comparator<T> comparator) {
        return (t, s) -> IS_NOT_NULL.test(t) && comparator.compare(t, s);
    }

    private static TypedObject getTypedValue(BulletRecord record, Type type, Value value) {
        TypedObject result = null;
        String valueString = value.getValue();
        Type newType = value.getType();
        switch (value.getKind()) {
            case FIELD:
                result = newType == null ? TypedObject.typeCastFromObject(type, record.extractField(valueString))
                                         : extractTypedObject(valueString, record).forceCast(newType);
                break;
            case VALUE:
                result = newType == null ? TypedObject.typeCast(type, valueString) : TypedObject.forceCast(newType, valueString);
                break;
        }
        return result;
    }

    private static boolean performRelational(BulletRecord record, ObjectFilterClause clause) {
        Clause.Operation operator = clause.getOperation();
        if (isEmpty(clause.getValues())) {
            return true;
        }
        TypedObject object = extractTypedObject(clause.getField(), record);
        if (operator == Clause.Operation.REGEX_LIKE) {
            return REGEX_LIKE.compare(object, clause.getPatterns().stream());
        }


        Type type;
        if (operator == Clause.Operation.SIZE_IS) {
            type = Type.INTEGER;
        } else if (operator == Clause.Operation.CONTAINS_KEY) {
            type = Type.STRING;
        } else if (operator == Clause.Operation.CONTAINS_VALUE) {
            type = object.getPrimitiveType();
        } else {
            type = object.getType();
        }
        return COMPARATORS.get(operator).compare(object, cast(record, type, clause.getValues()));
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
     * @return The resulting boolean from applying the clause on the record.
     */
    public static boolean perform(BulletRecord record, Clause clause) {
        // Rather than define another hierarchy of Clause -> ObjectFilterClause, LogicalClause evaluators, we'll eat
        // the cost of violating polymorphism in this one spot.
        // We do not want processing logic in FilterClause or LogicalClause, otherwise we could put the appropriate
        // methods in those classes.
        try {
            // All StringFilterClauses have been rewritten at Query configuration.
            if (clause instanceof ObjectFilterClause) {
                return performRelational(record, (ObjectFilterClause) clause);
            } else {
                return performLogical(record, (LogicalClause) clause);
            }
        } catch (RuntimeException e) {
            log.error("Unable to perform filter {} to record {}", clause, record);
            log.error("Skipping due to", e);
            return false;
        }
    }
}

