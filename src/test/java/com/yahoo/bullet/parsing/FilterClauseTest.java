/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletError;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.yahoo.bullet.parsing.Clause.Operation.EQUALS;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class FilterClauseTest {
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
    public void testInitializeNoOperation() {
        FilterClause filterClause = new FilterClause();
        Optional<List<BulletError>> optionalErrors = filterClause.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        List<BulletError> errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Clause.OPERATION_MISSING);
    }

    @Test
    public void testInitializeWithOperation() {
        FilterClause filterClause = new FilterClause();
        filterClause.setOperation(Clause.Operation.EQUALS);
        Optional<List<BulletError>> optionalErrors = filterClause.initialize();
        Assert.assertFalse(optionalErrors.isPresent());
    }

    @Test
    public void testInitializeForPatterns() {
        FilterClause filterClause = new FilterClause();
        filterClause.setOperation(Clause.Operation.REGEX_LIKE);
        filterClause.setValues(singletonList(".g.*"));
        Assert.assertNull(filterClause.getPatterns());
        Optional<List<BulletError>> optionalErrors = filterClause.initialize();
        Assert.assertFalse(optionalErrors.isPresent());
        List<Pattern> actual = filterClause.getPatterns();
        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.size(), 1);
        Assert.assertEquals(actual.get(0).pattern(), ".g.*");
    }

    @Test
    public void testInitializeForBadPatterns() {
        FilterClause filterClause = new FilterClause();
        filterClause.setOperation(Clause.Operation.REGEX_LIKE);
        filterClause.setValues(singletonList("*TEST*"));
        Assert.assertNull(filterClause.getPatterns());
        Optional<List<BulletError>> optionalErrors = filterClause.initialize();
        Assert.assertFalse(optionalErrors.isPresent());
        Assert.assertNotNull(filterClause.getPatterns());
        Assert.assertTrue(filterClause.getPatterns().isEmpty());
    }

    @Test
    public void testDefaults() {
        FilterClause filterClause = new FilterClause();
        Assert.assertNull(filterClause.getOperation());
        Assert.assertNull(filterClause.getField());
        Assert.assertNull(filterClause.getValues());
    }
}
