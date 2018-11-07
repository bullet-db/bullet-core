/*
 *  Copyright 2017, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.common.BulletConfig;
import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.typesystem.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.yahoo.bullet.parsing.Clause.Operation.EQUALS;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class ObjectFilterClauseTest {
    @Test
    public void testToString() {
        FilterClause filterClause = new ObjectFilterClause();
        Assert.assertEquals(filterClause.toString(), "{operation: null, field: null, values: null}");
        filterClause.setValues(emptyList());
        Assert.assertEquals(filterClause.toString(), "{operation: null, field: null, values: []}");
        filterClause.setOperation(EQUALS);
        Assert.assertEquals(filterClause.toString(), "{operation: EQUALS, field: null, values: []}");
        filterClause.setField("foo");
        Assert.assertEquals(filterClause.toString(), "{operation: EQUALS, field: foo, values: []}");
        Value value1 = new Value(Value.Kind.VALUE, "a");
        Value value2 = new Value(Value.Kind.FIELD, "b");
        filterClause.setValues(asList(value1, value2));
        Assert.assertEquals(filterClause.toString(), "{operation: EQUALS, field: foo, values: [{kind: VALUE, value: a, type: null}, {kind: FIELD, value: b, type: null}]}");
    }

    @Test
    public void testConfigureForPatterns() {
        FilterClause filterClause = new ObjectFilterClause();
        filterClause.setOperation(Clause.Operation.REGEX_LIKE);
        filterClause.setValues(singletonList(new Value(Value.Kind.VALUE, ".g.*")));
        Assert.assertNull(filterClause.getPatterns());
        filterClause.configure(new BulletConfig());
        Assert.assertFalse(filterClause.initialize().isPresent());
        List<Pattern> actual = filterClause.getPatterns();
        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.size(), 1);
        Assert.assertEquals(actual.get(0).pattern(), ".g.*");
    }

    @Test
    public void testConfigureForBadPatterns() {
        FilterClause filterClause = new ObjectFilterClause();
        filterClause.setOperation(Clause.Operation.REGEX_LIKE);
        filterClause.setValues(singletonList(new Value(Value.Kind.VALUE, "*TEST*")));
        Assert.assertNull(filterClause.getPatterns());
        filterClause.configure(new BulletConfig());
        Assert.assertFalse(filterClause.initialize().isPresent());
        Assert.assertNotNull(filterClause.getPatterns());
        Assert.assertTrue(filterClause.getPatterns().isEmpty());
    }

    @Test
    public void testInitializeNoOperation() {
        FilterClause filterClause = new ObjectFilterClause();
        Optional<List<BulletError>> optionalErrors = filterClause.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        List<BulletError> errors = optionalErrors.get();
        Assert.assertEquals(errors.size(), 1);
        Assert.assertEquals(errors.get(0), Clause.OPERATION_MISSING);
    }

    @Test
    public void testInitializeWithInvalidValue() {
        FilterClause filterClause = new ObjectFilterClause();
        filterClause.setOperation(Clause.Operation.EQUALS);
        Value value = new Value(null, "1");
        filterClause.setValues(Collections.singletonList(value));
        Optional<List<BulletError>> optionalErrors = filterClause.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        List<BulletError> errors = optionalErrors.get();
        Assert.assertEquals(errors.get(0), Value.VALUE_OBJECT_REQUIRES_NOT_NULL_KIND_ERROR);

        value = new Value(Value.Kind.VALUE, null);
        filterClause.setValues(Collections.singletonList(value));
        optionalErrors = filterClause.initialize();
        Assert.assertTrue(optionalErrors.isPresent());
        errors = optionalErrors.get();
        Assert.assertEquals(errors.get(0), Value.VALUE_OBJECT_REQUIRES_NOT_NULL_VALUE_ERROR);

    }

    @Test
    public void testInitializeWithOperation() {
        FilterClause filterClause = new ObjectFilterClause();
        filterClause.setOperation(Clause.Operation.EQUALS);
        Optional<List<BulletError>> optionalErrors = filterClause.initialize();
        Assert.assertFalse(optionalErrors.isPresent());
    }

    @Test
    public void testDefaults() {
        FilterClause filterClause = new ObjectFilterClause();
        Assert.assertNull(filterClause.getOperation());
        Assert.assertNull(filterClause.getField());
        Assert.assertNull(filterClause.getValues());
    }

    @Test
    public void testHasNull() {
        ObjectFilterClause filterClause = new ObjectFilterClause();
        Value valueA = new Value(null, Type.NULL_EXPRESSION);
        Assert.assertTrue(filterClause.hasNull(valueA));
        Value valueB = new Value(Value.Kind.VALUE, Type.NULL_EXPRESSION);
        Assert.assertTrue(filterClause.hasNull(valueB));
        Value valueC = new Value(Value.Kind.VALUE, Type.NULL_EXPRESSION, Type.NULL);
        Assert.assertTrue(filterClause.hasNull(valueC));
        Value valueD = new Value(Value.Kind.FIELD, Type.NULL_EXPRESSION);
        Assert.assertTrue(filterClause.hasNull(valueD));
        Value valueE = new Value(Value.Kind.VALUE, Type.NULL_EXPRESSION, Type.STRING);
        Assert.assertFalse(filterClause.hasNull(valueE));
    }
}
