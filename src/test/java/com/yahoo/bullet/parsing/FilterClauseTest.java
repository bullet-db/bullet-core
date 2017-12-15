/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.querying.FilterOperations.FilterType;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.querying.FilterOperations.FilterType.EQUALS;
import static com.yahoo.bullet.querying.FilterOperations.FilterType.GREATER_EQUALS;
import static com.yahoo.bullet.querying.FilterOperations.FilterType.GREATER_THAN;
import static com.yahoo.bullet.querying.FilterOperations.FilterType.LESS_EQUALS;
import static com.yahoo.bullet.querying.FilterOperations.FilterType.LESS_THAN;
import static com.yahoo.bullet.querying.FilterOperations.FilterType.NOT_EQUALS;
import static com.yahoo.bullet.querying.FilterOperations.FilterType.REGEX_LIKE;
import static com.yahoo.bullet.parsing.QueryUtils.makeClause;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class FilterClauseTest {
    public static FilterClause getFieldFilter(String field, FilterType operation, String... values) {
        return (FilterClause) makeClause(field, values == null ? null : asList(values), operation);
    }

    public static FilterClause getFieldFilter(FilterType operation, String... values) {
        return (FilterClause) makeClause("field", values == null ? null : asList(values), operation);
    }

    @Test
    public void testDefaults() {
        FilterClause filterClause = new FilterClause();
        Assert.assertNull(filterClause.getOperation());
        Assert.assertNull(filterClause.getField());
        Assert.assertNull(filterClause.getValues());

        // Without an operation, filter always returns true
        Assert.assertTrue(filterClause.check(RecordBox.get().getRecord()));

        // Without an operation, filter always returns true even with set values
        filterClause.setValues(singletonList("a"));
        Assert.assertTrue(filterClause.check(RecordBox.get().getRecord()));

        // With non-empty values, filter always returns true
        filterClause.setOperation(EQUALS);
        filterClause.setValues(emptyList());
        Assert.assertTrue(filterClause.check(RecordBox.get().getRecord()));
    }

    @Test
    public void testMissingFields() {
        FilterClause filterClause = new FilterClause();
        Assert.assertNull(filterClause.getOperation());
        filterClause.setOperation(EQUALS);
        Assert.assertTrue(filterClause.check(RecordBox.get().getRecord()));
        filterClause.setField("field");
        Assert.assertTrue(filterClause.check(RecordBox.get().add("field", "foo").getRecord()));
        filterClause.setValues(singletonList("bar"));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("field", "foo").getRecord()));
    }

    @Test
    public void testNullComparison() {
        FilterClause filterClause = getFieldFilter(EQUALS);
        Assert.assertTrue(filterClause.check(RecordBox.get().getRecord()));
        filterClause = getFieldFilter("map_field", NOT_EQUALS, "null");
        // This should add a null map_field
        Assert.assertFalse(filterClause.check(RecordBox.get().addMap("map_field", null, null).getRecord()));
        filterClause = getFieldFilter("map_field", EQUALS, "null");
        Assert.assertTrue(filterClause.check(RecordBox.get().getRecord()));
    }

    @Test
    public void testEquals() {
        FilterClause filterClause = getFieldFilter(EQUALS, "foo", "bar");
        Assert.assertFalse(filterClause.check(RecordBox.get().getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("field", "baz").getRecord()));
        Assert.assertTrue(filterClause.check(RecordBox.get().add("field", "foo").getRecord()));
        Assert.assertTrue(filterClause.check(RecordBox.get().add("field", "bar").getRecord()));
    }

    @Test
    public void testGreaterThan() {
        FilterClause filterClause = getFieldFilter("timestamp", GREATER_THAN, "3");

        // "NULL" is not > 3
        Assert.assertFalse(filterClause.check(RecordBox.get().getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("timestamp", "2").getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("timestamp", "3").getRecord()));
        Assert.assertTrue(filterClause.check(RecordBox.get().add("timestamp", "4").getRecord()));
    }

    @Test
    public void testLessThan() {
        FilterClause filterClause = getFieldFilter("timestamp", LESS_THAN, "3");
        // "NULL" is not > 3
        Assert.assertFalse(filterClause.check(RecordBox.get().getRecord()));
        Assert.assertTrue(filterClause.check(RecordBox.get().add("timestamp", "2").getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("timestamp", "3").getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("timestamp", "4").getRecord()));
    }

    @Test
    public void testGreaterThanEquals() {
        FilterClause filterClause = getFieldFilter("timestamp", GREATER_EQUALS, "3");
        // "NULL" is not >= 3
        Assert.assertFalse(filterClause.check(RecordBox.get().getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("timestamp", "2").getRecord()));
        Assert.assertTrue(filterClause.check(RecordBox.get().add("timestamp", "3").getRecord()));
        Assert.assertTrue(filterClause.check(RecordBox.get().add("timestamp", "4").getRecord()));
    }

    @Test
    public void testLessThanEquals() {
        FilterClause filterClause = getFieldFilter("timestamp", LESS_EQUALS, "3");
        // "NULL" is not <= 3
        Assert.assertFalse(filterClause.check(RecordBox.get().getRecord()));
        Assert.assertTrue(filterClause.check(RecordBox.get().add("timestamp", "2").getRecord()));
        Assert.assertTrue(filterClause.check(RecordBox.get().add("timestamp", "3").getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("timestamp", "4").getRecord()));
    }

    @Test
    public void testNotEquals() {
        FilterClause filterClause = getFieldFilter(NOT_EQUALS, "foo", "bar", "null");
        Assert.assertFalse(filterClause.check(RecordBox.get().getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("field", "foo").getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("field", "bar").getRecord()));
        Assert.assertTrue(filterClause.check(RecordBox.get().add("field", "baz").getRecord()));
    }

    @Test
    public void testRegexLike() {
        FilterClause filterClause = getFieldFilter("id", REGEX_LIKE, "1.*2", "[1-5]+0*3");
        Assert.assertFalse(filterClause.check(RecordBox.get().getRecord()));
        Assert.assertTrue(filterClause.check(RecordBox.get().add("id", "12").getRecord()));
        Assert.assertTrue(filterClause.check(RecordBox.get().add("id", "1131112").getRecord()));
        Assert.assertTrue(filterClause.check(RecordBox.get().add("id", "55003").getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("id", "55001").getRecord()));
    }

    @Test
    public void testRegexLikeNull() {
        FilterClause filterClause = getFieldFilter("id", REGEX_LIKE, "nu.*");
        Assert.assertFalse(filterClause.check(RecordBox.get().getRecord()));
        Assert.assertTrue(filterClause.check(RecordBox.get().add("id", "nu").getRecord()));
    }

    @Test
    public void testBadRegex() {
        FilterClause filterClause = getFieldFilter("id", REGEX_LIKE, "*TEST*");
        Assert.assertFalse(filterClause.check(RecordBox.get().getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("id", "TEST").getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("id", "*TEST*").getRecord()));
    }

    @Test
    public void testComparisonNestedField() {
        FilterClause filterClause = getFieldFilter("demographic_map.age", GREATER_THAN, "30");

        // "null" is not > 30
        Assert.assertFalse(filterClause.check(RecordBox.get().getRecord()));
        RecordBox box = RecordBox.get();
        box.addMap("demographic_map", Pair.of("age", "3"));
        Assert.assertFalse(filterClause.check(box.getRecord()));
        box.addMap("demographic_map", Pair.of("age", "30"));
        Assert.assertFalse(filterClause.check(box.getRecord()));
        box.addMap("demographic_map", Pair.of("age", "31"));
        Assert.assertTrue(filterClause.check(box.getRecord()));
    }

    @Test
    public void testComparisonBooleanMap() {
        FilterClause filterClause = getFieldFilter("filter_map.is_fake_event", EQUALS, "true");

        // "null" is not true
        Assert.assertFalse(filterClause.check(RecordBox.get().getRecord()));
        RecordBox box = RecordBox.get();
        Assert.assertFalse(filterClause.check(box.getRecord()));
        box.addMap("filter_map", Pair.of("is_fake_event", true));
        Assert.assertTrue(filterClause.check(box.getRecord()));
        box.addMap("filter_map", Pair.of("is_fake_event", false));
        Assert.assertFalse(filterClause.check(box.getRecord()));
    }

    @Test
    public void testComparisonUncastable() {
        FilterClause filterClause = getFieldFilter("unreal", EQUALS, "1.23", "4.56");
        RecordBox box = RecordBox.get().add("unreal", 123L);
        // Trying to cast
        Assert.assertFalse(filterClause.check(box.getRecord()));
    }

    @Test
    public void testToString() {
        FilterClause filterClause = new FilterClause();
        Assert.assertEquals(filterClause.toString(), "{operation: null, field: null, values: null}");
        filterClause.setValues(emptyList());
        Assert.assertEquals(filterClause.toString(), "{operation: null, field: null, values: []}");
        filterClause.setOperation(EQUALS);
        Assert.assertEquals(filterClause.toString(), "{operation: EQUALS, field: null, values: []}");
        filterClause.setField("foo");
        Assert.assertEquals(filterClause.toString(), "{operation: EQUALS, field: foo, values: []}");
        filterClause.setValues(asList("a", "b"));
        Assert.assertEquals(filterClause.toString(), "{operation: EQUALS, field: foo, values: [a, b]}");
    }

    @Test
    public void testValidate() {
        FilterClause filterClause = new FilterClause();
        Optional<List<Error>> errors = filterClause.initialize();
        // currently FilterClause.normalize() does nothing
        Assert.assertFalse(errors.isPresent());
    }

    @Test
    public void testNullInValues() {
        FilterClause filterClause = getFieldFilter(EQUALS, null, "foo", null);
        Assert.assertFalse(filterClause.check(RecordBox.get().getRecord()));
        Assert.assertTrue(filterClause.check(RecordBox.get().add("field", "foo").getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("field", "bar").getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().addNull("field").getRecord()));
        Assert.assertFalse(filterClause.check(RecordBox.get().add("field", Type.NULL_EXPRESSION).getRecord()));
    }
}
