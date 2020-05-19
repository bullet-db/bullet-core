/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query;

import com.yahoo.bullet.query.expressions.ValueExpression;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FieldTest {
    @Test
    public void testEquals() {
        Field fieldA = new Field("abc", new ValueExpression(5));
        Field fieldB = new Field("def", new ValueExpression(5));
        Field fieldC = new Field("abc", new ValueExpression(1));
        Field fieldD = new Field("abc", new ValueExpression(5));

        Assert.assertEquals(fieldA, fieldA);
        Assert.assertNotEquals(fieldA, fieldB);
        Assert.assertNotEquals(fieldA, fieldC);
        Assert.assertEquals(fieldA, fieldD);

        // coverage
        Assert.assertFalse(fieldA.equals(0));
    }

    @Test
    public void testHashCode() {
        Field fieldA = new Field("abc", new ValueExpression(5));
        Field fieldB = new Field("def", new ValueExpression(5));
        Field fieldC = new Field("abc", new ValueExpression(1));
        Field fieldD = new Field("abc", new ValueExpression(5));

        Assert.assertNotEquals(fieldA.hashCode(), fieldB.hashCode());
        Assert.assertNotEquals(fieldA.hashCode(), fieldC.hashCode());
        Assert.assertEquals(fieldA.hashCode(), fieldD.hashCode());
    }

    @Test
    public void testToString() {
        Field field = new Field("abc", new ValueExpression(5));
        Assert.assertEquals(field.toString(), "{name: abc, value: {value: 5, type: INTEGER}}");
    }
}
