/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.parsing.Clause.Operation.AND;
import static com.yahoo.bullet.parsing.Clause.Operation.EQUALS;
import static com.yahoo.bullet.parsing.Clause.Operation.NOT;
import static com.yahoo.bullet.parsing.Clause.Operation.OR;
import static com.yahoo.bullet.parsing.Clause.Operation.REGEX_LIKE;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class LogicalClauseTest {
    @Test
    public void testDefaults() {
        LogicalClause logicalClause = new LogicalClause();
        Assert.assertNull(logicalClause.getOperation());
        Assert.assertNull(logicalClause.getClauses());
    }

    @Test
    public void testConfigure() {
        LogicalClause logicalClause = new LogicalClause();
        logicalClause.setOperation(AND);
        FilterClause filterClause =  new FilterClause();
        filterClause.setField("id");
        filterClause.setOperation(REGEX_LIKE);
        filterClause.setValues(singletonList("f.*"));
        logicalClause.setClauses(singletonList(filterClause));

        Assert.assertNull(filterClause.getPatterns());
        logicalClause.configure(new BulletConfig());
        Assert.assertNotNull(filterClause.getPatterns());
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
    public void testInitializeWithNoOperation() {
        LogicalClause clause = new LogicalClause();
        Optional<List<BulletError>> optionalErrors = clause.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        List<BulletError> errors = optionalErrors.get();
        Assert.assertEquals(errors.get(0), Clause.OPERATION_MISSING);
    }
    @Test
    public void testInitializeWithOperation() {
        LogicalClause clause = new LogicalClause();
        clause.setOperation(AND);;
        Optional<List<BulletError>> errors = clause.initialize();
        Assert.assertFalse(errors.isPresent());
    }
}
