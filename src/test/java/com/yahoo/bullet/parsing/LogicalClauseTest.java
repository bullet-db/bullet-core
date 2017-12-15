/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.querying.FilterOperations.FilterType;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.RecordBox;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.querying.FilterOperations.FilterType.AND;
import static com.yahoo.bullet.querying.FilterOperations.FilterType.EQUALS;
import static com.yahoo.bullet.querying.FilterOperations.FilterType.GREATER_THAN;
import static com.yahoo.bullet.querying.FilterOperations.FilterType.NOT;
import static com.yahoo.bullet.querying.FilterOperations.FilterType.NOT_EQUALS;
import static com.yahoo.bullet.querying.FilterOperations.FilterType.OR;
import static com.yahoo.bullet.parsing.QueryUtils.makeClause;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class LogicalClauseTest {
    public static Clause clause(String field, FilterType operation, String... values) {
        return makeClause(field, values == null ? null : asList(values), operation);
    }

    public static LogicalClause clause(FilterType operation, Clause... clauses) {
        return (LogicalClause) makeClause(operation, clauses);
    }

    @Test
    public void testDefaults() {
        LogicalClause logicalClause = new LogicalClause();
        Assert.assertNull(logicalClause.getOperation());
        Assert.assertNull(logicalClause.getClauses());

        // Without an operation, logical always returns true
        Assert.assertTrue(logicalClause.check(RecordBox.get().getRecord()));

        // Without an operation, logical always returns true even with set clauses
        logicalClause.setClauses(singletonList(makeClause("map_field.foo", asList("a", "b"), EQUALS)));
        Assert.assertTrue(logicalClause.check(RecordBox.get().getRecord()));

        // With empty values, logical always returns true
        logicalClause.setOperation(AND);
        logicalClause.setClauses(emptyList());
        Assert.assertTrue(logicalClause.check(RecordBox.get().getRecord()));
    }

    @Test
    public void testAnd() {
        // id IN [1, 3, 5] AND field NOT IN ["foo"]
        LogicalClause logicalClause = clause(AND,
                                             clause("id", EQUALS, "1", "3", "5"),
                                             clause("field", NOT_EQUALS, "foo"));
        BulletRecord recordA = RecordBox.get().add("id", 5).add("field", "foo").getRecord();
        BulletRecord recordB = RecordBox.get().add("id", 3).add("field", "bar").getRecord();
        BulletRecord recordC = RecordBox.get().add("id", 3).getRecord();
        BulletRecord recordD = RecordBox.get().add("field", "baz").getRecord();
        BulletRecord recordE = RecordBox.get().getRecord();

        Assert.assertFalse(logicalClause.check(recordA));
        Assert.assertTrue(logicalClause.check(recordB));
        Assert.assertTrue(logicalClause.check(recordC)); // field != "foo" is true because field is null
        Assert.assertFalse(logicalClause.check(recordD));
        Assert.assertFalse(logicalClause.check(recordE));
    }

    @Test
    public void testOr() {
        // id IN [1, 3, 5] OR field NOT IN ["foo"]
        LogicalClause logicalClause = clause(OR,
                                              clause("id", EQUALS, "1", "3", "5"),
                                              clause("field", NOT_EQUALS, "foo"));
        BulletRecord recordA = RecordBox.get().add("id", 5).add("field", "foo").getRecord();
        BulletRecord recordB = RecordBox.get().add("id", 3).add("field", "bar").getRecord();
        BulletRecord recordC = RecordBox.get().add("id", 3).getRecord();
        BulletRecord recordD = RecordBox.get().add("field", "baz").getRecord();
        BulletRecord recordE = RecordBox.get().add("id", 2).add("field", "foo").getRecord();
        BulletRecord recordF = RecordBox.get().getRecord();

        Assert.assertTrue(logicalClause.check(recordA));
        Assert.assertTrue(logicalClause.check(recordB));
        Assert.assertTrue(logicalClause.check(recordC));
        Assert.assertTrue(logicalClause.check(recordD));
        Assert.assertFalse(logicalClause.check(recordE));
        Assert.assertTrue(logicalClause.check(recordF)); // field != "foo" is true because field is null
    }

    @Test
    public void testNot() {
        // NOT(id IN [1, 3, 5] OR field NOT IN ["foo"])
        LogicalClause logicalClause = clause(NOT,
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
        Assert.assertFalse(logicalClause.check(recordA));
        Assert.assertFalse(logicalClause.check(recordB));
        Assert.assertFalse(logicalClause.check(recordC));
        Assert.assertFalse(logicalClause.check(recordD));
        Assert.assertTrue(logicalClause.check(recordE));
        Assert.assertFalse(logicalClause.check(recordF));
    }

    @Test
    public void testNotMultiple() {
        // NOT(id IN [1, 3, 5], field NOT IN ["foo"])
        // Only the id is negated
        LogicalClause logicalClause = clause(NOT,
                                             clause("id", EQUALS, "1", "3", "5"),
                                             clause("field", NOT_EQUALS, "foo"));
        BulletRecord recordA = RecordBox.get().add("id", 5).add("field", "foo").getRecord();
        BulletRecord recordB = RecordBox.get().add("id", 3).add("field", "bar").getRecord();
        BulletRecord recordC = RecordBox.get().add("id", 1).getRecord();
        BulletRecord recordD = RecordBox.get().add("field", "baz").getRecord();
        BulletRecord recordE = RecordBox.get().getRecord();

        Assert.assertFalse(logicalClause.check(recordA));
        Assert.assertFalse(logicalClause.check(recordB));
        Assert.assertFalse(logicalClause.check(recordC));
        Assert.assertTrue(logicalClause.check(recordD));
        Assert.assertTrue(logicalClause.check(recordE));
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
        LogicalClause logicalClause = clause(OR,
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

        Assert.assertTrue(logicalClause.check(recordA));
        Assert.assertFalse(logicalClause.check(recordB));
        Assert.assertFalse(logicalClause.check(recordC));
        Assert.assertFalse(logicalClause.check(recordD));
        Assert.assertFalse(logicalClause.check(recordE));
        Assert.assertFalse(logicalClause.check(recordF));
        Assert.assertTrue(logicalClause.check(recordG));
    }

    @Test
    public void testToString() {
        LogicalClause logicalClause = new LogicalClause();
        Assert.assertEquals(logicalClause.toString(), "{operation: null, clauses: null}");
        logicalClause.setClauses(emptyList());
        Assert.assertEquals(logicalClause.toString(), "{operation: null, clauses: []}");
        logicalClause.setOperation(OR);
        Assert.assertEquals(logicalClause.toString(), "{operation: OR, clauses: []}");

        FilterClause clauseA = new FilterClause();
        clauseA.setField("foo");
        clauseA.setOperation(EQUALS);
        clauseA.setValues(asList("a", "b"));
        LogicalClause clauseB = new LogicalClause();
        clauseB.setOperation(NOT);
        clauseB.setClauses(singletonList(clauseA));
        logicalClause.setClauses(asList(clauseA, clauseB));
        Assert.assertEquals(logicalClause.toString(), "{operation: OR, clauses: [" +
                                                        "{operation: EQUALS, field: foo, values: [a, b]}, " +
                                                        "{operation: NOT, " +
                                                         "clauses: [{operation: EQUALS, field: foo, values: [a, b]}]" +
                                                        "}" +
                                                      "]}");

    }

    @Test
    public void testValidate() {
        LogicalClause logicalClause = new LogicalClause();
        Optional<List<Error>> errors = logicalClause.initialize();
        // currently LogicalClause.normalize() does nothing
        Assert.assertFalse(errors.isPresent());
    }
}
