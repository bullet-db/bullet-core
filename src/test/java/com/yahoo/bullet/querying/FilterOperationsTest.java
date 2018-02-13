/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying;

import com.yahoo.bullet.parsing.Clause;
import com.yahoo.bullet.parsing.FilterClause;
import com.yahoo.bullet.parsing.LogicalClause;
import com.yahoo.bullet.querying.FilterOperations.Comparator;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.yahoo.bullet.parsing.FilterUtils.getFieldFilter;
import static com.yahoo.bullet.parsing.FilterUtils.makeClause;
import static com.yahoo.bullet.parsing.Clause.Operation.AND;
import static com.yahoo.bullet.parsing.Clause.Operation.EQUALS;
import static com.yahoo.bullet.parsing.Clause.Operation.GREATER_EQUALS;
import static com.yahoo.bullet.parsing.Clause.Operation.GREATER_THAN;
import static com.yahoo.bullet.parsing.Clause.Operation.LESS_EQUALS;
import static com.yahoo.bullet.parsing.Clause.Operation.LESS_THAN;
import static com.yahoo.bullet.parsing.Clause.Operation.NOT;
import static com.yahoo.bullet.parsing.Clause.Operation.NOT_EQUALS;
import static com.yahoo.bullet.parsing.Clause.Operation.OR;
import static com.yahoo.bullet.parsing.Clause.Operation.REGEX_LIKE;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class FilterOperationsTest {
    private static <T> Stream<TypedObject> make(TypedObject source, String... items) {
        return FilterOperations.cast(source, asList(items));
    }

    private static Stream<Pattern> makePattern(String... items) {
        return Arrays.stream(items).map(Pattern::compile);
    }

    private static Comparator<TypedObject> comparatorFor(Clause.Operation operation) {
        return FilterOperations.COMPARATORS.get(operation);
    }

    private static Clause clause(String field, Clause.Operation operation, String... values) {
        return makeClause(field, values == null ? null : asList(values), operation);
    }

    private static LogicalClause clause(Clause.Operation operation, Clause... clauses) {
        return (LogicalClause) makeClause(operation, clauses);
    }

    @Test
    public void testComparatorUnsupportedType() {
        TypedObject object = new TypedObject(Type.MAP, singletonMap("foo", "bar"));
        // foo cannot be casted to map, so eq will return false (values will be empty)
        Assert.assertFalse(comparatorFor(EQUALS).compare(object, make(object, "foo")));
        // foo cannot be casted to map, so neq will return true (values will be empty)
        Assert.assertTrue(comparatorFor(NOT_EQUALS).compare(object, make(object, "foo")));
        Assert.assertFalse(comparatorFor(GREATER_THAN).compare(object, make(object, "foo")));
        Assert.assertFalse(comparatorFor(GREATER_EQUALS).compare(object, make(object, "foo")));
        Assert.assertFalse(comparatorFor(LESS_THAN).compare(object, make(object, "foo")));
        Assert.assertFalse(comparatorFor(LESS_EQUALS).compare(object, make(object, "foo")));
        Assert.assertFalse(FilterOperations.REGEX_LIKE.compare(object, makePattern("foo")));
    }

    @Test
    public void testOnDoubles() {
        TypedObject object = new TypedObject(Double.valueOf("1.234"));
        Assert.assertTrue(comparatorFor(EQUALS).compare(object, make(object, "1.234", "4.343", "foo")));
        Assert.assertFalse(comparatorFor(EQUALS).compare(object, make(object, "4.343")));
    }

    @Test
    public void testComparatorUncastable() {
        TypedObject object = new TypedObject(Double.valueOf("1.234"));
        Assert.assertFalse(comparatorFor(EQUALS).compare(object, make(object, "foo")));
    }

    @Test
    public void testNulls() {
        TypedObject object = new TypedObject(Type.NULL, null);
        Assert.assertFalse(comparatorFor(EQUALS).compare(object, make(object, "foo")));
        Assert.assertTrue(comparatorFor(EQUALS).compare(object, make(object, "null")));
        Assert.assertTrue(comparatorFor(EQUALS).compare(object, make(object, "NULL")));
        Assert.assertTrue(comparatorFor(EQUALS).compare(object, make(object, "Null")));
        Assert.assertTrue(comparatorFor(NOT_EQUALS).compare(object, make(object, "foo")));
        Assert.assertFalse(comparatorFor(NOT_EQUALS).compare(object, make(object, "null")));
        Assert.assertFalse(comparatorFor(NOT_EQUALS).compare(object, make(object, "NULL")));
        Assert.assertFalse(comparatorFor(NOT_EQUALS).compare(object, make(object, "Null")));
        Assert.assertFalse(comparatorFor(GREATER_THAN).compare(object, make(object, "foo")));
        Assert.assertFalse(comparatorFor(LESS_THAN).compare(object, make(object, "foo")));
        Assert.assertFalse(comparatorFor(GREATER_EQUALS).compare(object, make(object, "foo")));
        Assert.assertFalse(comparatorFor(LESS_EQUALS).compare(object, make(object, "foo")));
        Assert.assertFalse(FilterOperations.REGEX_LIKE.compare(object, makePattern("foo")));
        Assert.assertFalse(FilterOperations.REGEX_LIKE.compare(object, makePattern("null")));
        Assert.assertFalse(FilterOperations.REGEX_LIKE.compare(object, makePattern("nu.*")));
    }

    @Test
    public void testMixedTypes() {
        TypedObject object = new TypedObject(2.34);
        Assert.assertTrue(comparatorFor(EQUALS).compare(object, make(object, "foo", "2.34")));
        Assert.assertFalse(comparatorFor(EQUALS).compare(object, make(object, "foo", "3.42")));

        Assert.assertTrue(comparatorFor(NOT_EQUALS).compare(object, make(object, "baz", "bar")));
        Assert.assertFalse(comparatorFor(NOT_EQUALS).compare(object, make(object, "baz", "2.34")));

        Assert.assertTrue(comparatorFor(GREATER_THAN).compare(object, make(object, "baz", "2.1")));
        Assert.assertFalse(comparatorFor(GREATER_THAN).compare(object, make(object, "baz", "2.34")));
        Assert.assertFalse(comparatorFor(GREATER_THAN).compare(object, make(object, "baz", "2.4")));

        Assert.assertFalse(comparatorFor(LESS_THAN).compare(object, make(object, "baz", "2.1")));
        Assert.assertFalse(comparatorFor(LESS_THAN).compare(object, make(object, "baz", "2.34")));
        Assert.assertTrue(comparatorFor(LESS_THAN).compare(object, make(object, "baz", "2.4")));

        Assert.assertTrue(comparatorFor(GREATER_EQUALS).compare(object, make(object, "baz", "2.1")));
        Assert.assertTrue(comparatorFor(GREATER_EQUALS).compare(object, make(object, "baz", "2.34")));
        Assert.assertFalse(comparatorFor(GREATER_EQUALS).compare(object, make(object, "baz", "2.4")));

        Assert.assertFalse(comparatorFor(LESS_EQUALS).compare(object, make(object, "baz", "2.1")));
        Assert.assertTrue(comparatorFor(LESS_EQUALS).compare(object, make(object, "baz", "2.34")));
        Assert.assertTrue(comparatorFor(LESS_EQUALS).compare(object, make(object, "baz", "2.4")));
    }

    @Test
    public void testEquality() {
        TypedObject object = new TypedObject("foo");
        Assert.assertTrue(comparatorFor(EQUALS).compare(object, make(object, "foo", "bar")));
        Assert.assertFalse(comparatorFor(EQUALS).compare(object, make(object, "baz", "bar")));
        // Will become a string
        object = new TypedObject(singletonList("foo"));
        Assert.assertFalse(comparatorFor(EQUALS).compare(object, make(object, "foo", "bar")));
        // Can't be casted to a list, so the equality check will fail
        object = new TypedObject(Type.LIST, singletonList("foo"));
        Assert.assertFalse(comparatorFor(EQUALS).compare(object, make(object, "foo", "bar")));
    }

    @Test
    public void testInEquality() {
        TypedObject object = new TypedObject(1L);
        Assert.assertFalse(comparatorFor(NOT_EQUALS).compare(object, make(object, "1", "2")));
        Assert.assertTrue(comparatorFor(NOT_EQUALS).compare(object, make(object, "2", "3")));
        // Will become a string
        object = new TypedObject(singletonList("1"));
        Assert.assertTrue(comparatorFor(NOT_EQUALS).compare(object, make(object, "1", "2")));
        // Can't be casted to a list, so the inequality check will pass
        object = new TypedObject(Type.LIST, singletonList("foo"));
        Assert.assertTrue(comparatorFor(NOT_EQUALS).compare(object, make(object, "foo", "bar")));
    }

    @Test
    public void testGreaterNumeric() {
        TypedObject object = new TypedObject(1L);
        Assert.assertTrue(comparatorFor(GREATER_THAN).compare(object, make(object, "0", "2")));
        Assert.assertFalse(comparatorFor(GREATER_THAN).compare(object, make(object, "1", "2")));
        Assert.assertTrue(comparatorFor(GREATER_EQUALS).compare(object, make(object, "0", "3")));
        Assert.assertFalse(comparatorFor(GREATER_EQUALS).compare(object, make(object, "2", "3")));

        // Will become UNKNOWN
        object = new TypedObject(singletonList("1"));
        Assert.assertFalse(comparatorFor(GREATER_THAN).compare(object, make(object, "1", "2")));
        Assert.assertFalse(comparatorFor(GREATER_EQUALS).compare(object, make(object, "1", "2")));

        // Will become UNKNOWN
        object = new TypedObject(Type.LIST, singletonList("1"));
        Assert.assertFalse(comparatorFor(GREATER_THAN).compare(object, make(object, "1", "2")));
        Assert.assertFalse(comparatorFor(GREATER_EQUALS).compare(object, make(object, "1", "2")));
    }

    @Test
    public void testLessNumeric() {
        TypedObject object = new TypedObject(1L);
        Assert.assertTrue(comparatorFor(LESS_THAN).compare(object, make(object, "-10", "2")));
        Assert.assertFalse(comparatorFor(LESS_THAN).compare(object, make(object, "-1", "0")));
        Assert.assertTrue(comparatorFor(LESS_EQUALS).compare(object, make(object, "0", "1")));
        Assert.assertFalse(comparatorFor(LESS_EQUALS).compare(object, make(object, "-2", "-3")));

        // Will become a string
        object = new TypedObject(singletonList("1"));
        Assert.assertFalse(comparatorFor(LESS_THAN).compare(object, make(object, "1", "2")));
        Assert.assertFalse(comparatorFor(LESS_EQUALS).compare(object, make(object, "1", "2")));
        // Can't be casted to a list, so the less check will fail
        object = new TypedObject(Type.LIST, singletonList("1"));
        Assert.assertFalse(comparatorFor(LESS_THAN).compare(object, make(object, "1", "2")));
        Assert.assertFalse(comparatorFor(LESS_EQUALS).compare(object, make(object, "1", "2")));
    }

    @Test
    public void testGreaterString() {
        TypedObject object = new TypedObject("foo");
        Assert.assertTrue(comparatorFor(GREATER_THAN).compare(object, make(object, "bravo", "2")));
        Assert.assertFalse(comparatorFor(GREATER_THAN).compare(object, make(object, "zulu", "xray")));
        Assert.assertTrue(comparatorFor(GREATER_EQUALS).compare(object, make(object, "alpha", "foo")));
        Assert.assertFalse(comparatorFor(GREATER_EQUALS).compare(object, make(object, "golf", "november")));
    }

    @Test
    public void testLessString() {
        TypedObject object = new TypedObject("foo");
        Assert.assertFalse(comparatorFor(LESS_THAN).compare(object, make(object, "bravo", "2")));
        Assert.assertTrue(comparatorFor(LESS_THAN).compare(object, make(object, "zulu", "xray")));
        Assert.assertTrue(comparatorFor(LESS_EQUALS).compare(object, make(object, "oscar", "foo")));
        Assert.assertFalse(comparatorFor(LESS_EQUALS).compare(object, make(object, "echo", "fi")));
    }

    @Test
    public void testRegexMatching() {
        List<Pattern> pattern = Stream.of(".g.", ".*foo.*").map(Pattern::compile).collect(Collectors.toList());
        Assert.assertTrue(FilterOperations.REGEX_LIKE.compare(new TypedObject("foo"), makePattern(".g.", ".*foo.*")));
        Assert.assertTrue(FilterOperations.REGEX_LIKE.compare(new TypedObject("food"), makePattern(".g.", ".*foo.*")));
        Assert.assertTrue(FilterOperations.REGEX_LIKE.compare(new TypedObject("egg"), makePattern(".g.", ".*foo.*")));
        Assert.assertFalse(FilterOperations.REGEX_LIKE.compare(new TypedObject("g"), makePattern(".g.", ".*foo.*")));
        Assert.assertFalse(FilterOperations.REGEX_LIKE.compare(new TypedObject("fgoo"), makePattern(".g.", ".*foo.*")));
        Assert.assertFalse(FilterOperations.REGEX_LIKE.compare(new TypedObject(Type.NULL, null), makePattern(".g.", ".*foo.*")));
    }

    //***************************************** Filter Clause *********************************************************

    @Test(expectedExceptions = NullPointerException.class)
    public void testFilterDefaults() {
        FilterClause clause = new FilterClause();
        clause.setValues(asList("foo", "bar"));
        // Without an operation, it is an error
        FilterOperations.perform(RecordBox.get().getRecord(), clause);
    }

    @Test
    public void testFilterDefaultsWithOperation() {
        FilterClause clause = new FilterClause();
        // With non-empty values, filter always returns true
        clause.setOperation(EQUALS);
        clause.setValues(emptyList());
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().getRecord(), clause));
    }

    @Test
    public void testFilterMissingFields() {
        FilterClause clause = new FilterClause();
        clause.setOperation(EQUALS);
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().getRecord(), clause));
        clause.setField("field");
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("field", "foo").getRecord(), clause));
        clause.setValues(singletonList("bar"));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("field", "foo").getRecord(), clause));
    }

    @Test
    public void testNullComparison() {
        FilterClause clause = getFieldFilter(EQUALS);
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().getRecord(), clause));

        clause = getFieldFilter("map_field", NOT_EQUALS, "null");
        // This should add a null map_field
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().addMap("map_field", null, null).getRecord(), clause));
        clause = getFieldFilter("map_field", EQUALS, "null");
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().getRecord(), clause));
    }

    @Test
    public void testEquals() {
        FilterClause clause = getFieldFilter(EQUALS, "foo", "bar");
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("field", "baz").getRecord(), clause));
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("field", "foo").getRecord(), clause));
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("field", "bar").getRecord(), clause));
    }

    @Test
    public void testGreaterThan() {
        FilterClause clause = getFieldFilter("timestamp", GREATER_THAN, "3");

        // "NULL" is not > 3
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("timestamp", "2").getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("timestamp", "3").getRecord(), clause));
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("timestamp", "4").getRecord(), clause));
    }

    @Test
    public void testLessThan() {
        FilterClause clause = getFieldFilter("timestamp", LESS_THAN, "3");
        // "NULL" is not > 3
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().getRecord(), clause));
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("timestamp", "2").getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("timestamp", "3").getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("timestamp", "4").getRecord(), clause));
    }

    @Test
    public void testGreaterThanEquals() {
        FilterClause clause = getFieldFilter("timestamp", GREATER_EQUALS, "3");
        // "NULL" is not >= 3
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("timestamp", "2").getRecord(), clause));
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("timestamp", "3").getRecord(), clause));
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("timestamp", "4").getRecord(), clause));
    }

    @Test
    public void testLessThanEquals() {
        FilterClause clause = getFieldFilter("timestamp", LESS_EQUALS, "3");
        // "NULL" is not <= 3
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().getRecord(), clause));
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("timestamp", "2").getRecord(), clause));
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("timestamp", "3").getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("timestamp", "4").getRecord(), clause));
    }

    @Test
    public void testNotEquals() {
        FilterClause clause = getFieldFilter(NOT_EQUALS, "foo", "bar", "null");
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("field", "foo").getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("field", "bar").getRecord(), clause));
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("field", "baz").getRecord(), clause));
    }

    @Test
    public void testRegexLike() {
        FilterClause clause = getFieldFilter("id", REGEX_LIKE, "1.*2", "[1-5]+0*3");
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().getRecord(), clause));
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("id", "12").getRecord(), clause));
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("id", "1131112").getRecord(), clause));
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("id", "55003").getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("id", "55001").getRecord(), clause));
    }

    @Test
    public void testRegexLikeNull() {
        FilterClause clause = getFieldFilter("id", REGEX_LIKE, "nu.*");
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().getRecord(), clause));
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("id", "nu").getRecord(), clause));
    }

    @Test
    public void testBadRegex() {
        FilterClause clause = getFieldFilter("id", REGEX_LIKE, "*TEST*");
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("id", "TEST").getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("id", "*TEST*").getRecord(), clause));
    }

    @Test
    public void testComparisonNestedField() {
        FilterClause clause = getFieldFilter("demographic_map.age", GREATER_THAN, "30");

        // "null" is not > 30
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().getRecord(), clause));
        RecordBox box = RecordBox.get();
        box.addMap("demographic_map", Pair.of("age", "3"));
        Assert.assertFalse(FilterOperations.perform(box.getRecord(), clause));
        box.addMap("demographic_map", Pair.of("age", "30"));
        Assert.assertFalse(FilterOperations.perform(box.getRecord(), clause));
        box.addMap("demographic_map", Pair.of("age", "31"));
        Assert.assertTrue(FilterOperations.perform(box.getRecord(), clause));
    }

    @Test
    public void testComparisonBooleanMap() {
        FilterClause clause = getFieldFilter("filter_map.is_fake_event", EQUALS, "true");

        // "null" is not true
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().getRecord(), clause));
        RecordBox box = RecordBox.get();
        Assert.assertFalse(FilterOperations.perform(box.getRecord(), clause));
        box.addMap("filter_map", Pair.of("is_fake_event", true));
        Assert.assertTrue(FilterOperations.perform(box.getRecord(), clause));
        box.addMap("filter_map", Pair.of("is_fake_event", false));
        Assert.assertFalse(FilterOperations.perform(box.getRecord(), clause));
    }

    @Test
    public void testComparisonUncastable() {
        FilterClause clause = getFieldFilter("unreal", EQUALS, "1.23", "4.56");
        RecordBox box = RecordBox.get().add("unreal", 123L);
        // Trying to cast
        Assert.assertFalse(FilterOperations.perform(box.getRecord(), clause));
    }

    @Test
    public void testNullInValues() {
        FilterClause clause = getFieldFilter(EQUALS, null, "foo", null);
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().getRecord(), clause));
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().add("field", "foo").getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("field", "bar").getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().addNull("field").getRecord(), clause));
        Assert.assertFalse(FilterOperations.perform(RecordBox.get().add("field", Type.NULL_EXPRESSION).getRecord(), clause));
    }

    //***************************************** Logical Clause *********************************************************

    @Test(expectedExceptions = NullPointerException.class)
    public void testLogicalNoOperation() {
        LogicalClause clause = new LogicalClause();
        clause.setClauses(asList(makeClause("foo", asList("foo", "bar"), EQUALS),
                                 makeClause("bar", asList("foo", "bar"), EQUALS)));
        Assert.assertNull(clause.getOperation());
        FilterOperations.perform(RecordBox.get().getRecord(), clause);
    }

    @Test
    public void testLogicalNoClauses() {
        LogicalClause clause = new LogicalClause();
        // With empty values, logical always returns true
        clause.setOperation(AND);
        clause.setClauses(emptyList());
        Assert.assertTrue(FilterOperations.perform(RecordBox.get().getRecord(), clause));
    }

    @Test
    public void testAnd() {
        // id IN [1, 3, 5] AND field NOT IN ["foo"]
        LogicalClause clause = clause(AND,
                                      clause("id", EQUALS, "1", "3", "5"),
                                      clause("field", NOT_EQUALS, "foo"));
        BulletRecord recordA = RecordBox.get().add("id", 5).add("field", "foo").getRecord();
        BulletRecord recordB = RecordBox.get().add("id", 3).add("field", "bar").getRecord();
        BulletRecord recordC = RecordBox.get().add("id", 3).getRecord();
        BulletRecord recordD = RecordBox.get().add("field", "baz").getRecord();
        BulletRecord recordE = RecordBox.get().getRecord();

        Assert.assertFalse(FilterOperations.perform(recordA, clause));
        Assert.assertTrue(FilterOperations.perform(recordB, clause));
        Assert.assertTrue(FilterOperations.perform(recordC, clause)); // field != "foo" is true because field is null
        Assert.assertFalse(FilterOperations.perform(recordD, clause));
        Assert.assertFalse(FilterOperations.perform(recordE, clause));
    }

    @Test
    public void testOr() {
        // id IN [1, 3, 5] OR field NOT IN ["foo"]
        LogicalClause clause = clause(OR,
                                      clause("id", EQUALS, "1", "3", "5"),
                                      clause("field", NOT_EQUALS, "foo"));
        BulletRecord recordA = RecordBox.get().add("id", 5).add("field", "foo").getRecord();
        BulletRecord recordB = RecordBox.get().add("id", 3).add("field", "bar").getRecord();
        BulletRecord recordC = RecordBox.get().add("id", 3).getRecord();
        BulletRecord recordD = RecordBox.get().add("field", "baz").getRecord();
        BulletRecord recordE = RecordBox.get().add("id", 2).add("field", "foo").getRecord();
        BulletRecord recordF = RecordBox.get().getRecord();

        Assert.assertTrue(FilterOperations.perform(recordA, clause));
        Assert.assertTrue(FilterOperations.perform(recordB, clause));
        Assert.assertTrue(FilterOperations.perform(recordC, clause));
        Assert.assertTrue(FilterOperations.perform(recordD, clause));
        Assert.assertFalse(FilterOperations.perform(recordE, clause));
        Assert.assertTrue(FilterOperations.perform(recordF, clause)); // field != "foo" is true because field is null
    }

    @Test
    public void testNot() {
        // NOT(id IN [1, 3, 5] OR field NOT IN ["foo"])
        LogicalClause clause = clause(NOT,
                                           clause(OR,
                                                  clause("id", EQUALS, "1", "3", "5"),
                                                  clause("field", NOT_EQUALS, "foo")));
        BulletRecord recordA = RecordBox.get().add("id", 5).add("field", "foo").getRecord();
        BulletRecord recordB = RecordBox.get().add("id", 3).add("field", "bar").getRecord();
        BulletRecord recordC = RecordBox.get().add("id", 3).getRecord();
        BulletRecord recordD = RecordBox.get().add("field", "baz").getRecord();
        BulletRecord recordE = RecordBox.get().add("id", 2).add("field", "foo").getRecord();
        BulletRecord recordF = RecordBox.get().getRecord();

        // Everything is the negation of the testOr checks
        Assert.assertFalse(FilterOperations.perform(recordA, clause));
        Assert.assertFalse(FilterOperations.perform(recordB, clause));
        Assert.assertFalse(FilterOperations.perform(recordC, clause));
        Assert.assertFalse(FilterOperations.perform(recordD, clause));
        Assert.assertTrue(FilterOperations.perform(recordE, clause));
        Assert.assertFalse(FilterOperations.perform(recordF, clause));
    }

    @Test
    public void testNotMultiple() {
        // NOT(id IN [1, 3, 5], field NOT IN ["foo"])
        // Only the id is negated
        LogicalClause clause = clause(NOT,
                                      clause("id", EQUALS, "1", "3", "5"),
                                      clause("field", NOT_EQUALS, "foo"));
        BulletRecord recordA = RecordBox.get().add("id", 5).add("field", "foo").getRecord();
        BulletRecord recordB = RecordBox.get().add("id", 3).add("field", "bar").getRecord();
        BulletRecord recordC = RecordBox.get().add("id", 1).getRecord();
        BulletRecord recordD = RecordBox.get().add("field", "baz").getRecord();
        BulletRecord recordE = RecordBox.get().getRecord();

        Assert.assertFalse(FilterOperations.perform(recordA, clause));
        Assert.assertFalse(FilterOperations.perform(recordB, clause));
        Assert.assertFalse(FilterOperations.perform(recordC, clause));
        Assert.assertTrue(FilterOperations.perform(recordD, clause));
        Assert.assertTrue(FilterOperations.perform(recordE, clause));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNested() {
        /*
         (
           field IN ["abc"]
           AND
           (
            (experience IN ["app", "tv"] AND id IN ["1, 2])
            OR
            mid > 10
           )
         )
         OR
         (
          demographic_map.age > 65
          AND
          filter_map.is_fake_event IN [true]
         )
        */
        LogicalClause clause = clause(OR,
                                             clause(AND,
                                                    clause("field", EQUALS, "abc"),
                                                    clause(OR,
                                                           clause(AND,
                                                                  clause("experience", EQUALS, "app", "tv"),
                                                                  clause("id", EQUALS, "1", "2")),
                                                    clause("mid", GREATER_THAN, "10"))),
                                             clause(AND,
                                                    clause("demographic_map.age", GREATER_THAN, "65"),
                                                    clause("filter_map.is_fake_event", EQUALS, "true")));


        // second clause is true : age > 65 and is_fake_event
        BulletRecord recordA = RecordBox.get().addMap("demographic_map", Pair.of("age", "67"))
                                              .addMap("filter_map", Pair.of("is_fake_event", true))
                                              .getRecord();
        // age > 65 and is_fake_event == null
        BulletRecord recordB = RecordBox.get().addMap("demographic_map", Pair.of("age", "67")).getRecord();

        // field != "abc"
        BulletRecord recordC = RecordBox.get().add("field", "cba").getRecord();

        // field == "abc" but experience != "app" or "tv"
        BulletRecord recordD = RecordBox.get().add("field", "abc").getRecord();

        // field == "abc", experience == "app" or "tv", mid == null
        BulletRecord recordE = RecordBox.get().add("field", "abc")
                                              .add("experience", "tv")
                                              .getRecord();

        // first clause is false : field == "abc", experience == "app" or "tv", mid < 10 and so is the second
        BulletRecord recordF = RecordBox.get().add("field", "abc")
                                              .add("experience", "tv")
                                              .add("mid", 9)
                                              .getRecord();

        // first clause is true : field == "abc", experience == "app" or "tv", mid > 10
        BulletRecord recordG = RecordBox.get().add("field", "abc")
                                              .add("experience", "tv")
                                              .add("mid", 12)
                                              .getRecord();

        Assert.assertTrue(FilterOperations.perform(recordA, clause));
        Assert.assertFalse(FilterOperations.perform(recordB, clause));
        Assert.assertFalse(FilterOperations.perform(recordC, clause));
        Assert.assertFalse(FilterOperations.perform(recordD, clause));
        Assert.assertFalse(FilterOperations.perform(recordE, clause));
        Assert.assertFalse(FilterOperations.perform(recordF, clause));
        Assert.assertTrue(FilterOperations.perform(recordG, clause));
    }
}
