/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.tablefunctions;

import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.querying.tablefunctors.ExplodeFunctor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ExplodeTest {
    @Test
    public void testExplodeTableFunction() {
        Explode tableFunction = new Explode(new FieldExpression("abc"), "foo", "bar", true);

        Assert.assertEquals(tableFunction.getType(), TableFunctionType.EXPLODE);
        Assert.assertEquals(tableFunction.getField(), new FieldExpression("abc"));
        Assert.assertEquals(tableFunction.getKeyAlias(), "foo");
        Assert.assertEquals(tableFunction.getValueAlias(), "bar");
        Assert.assertTrue(tableFunction.isOuter());
        Assert.assertTrue(tableFunction.getTableFunctor() instanceof ExplodeFunctor);
    }

    @Test
    public void testToString() {
        Explode tableFunction = new Explode(new FieldExpression("abc"), "foo", null, true);

        Assert.assertEquals(tableFunction.toString(), "{outer: true, type: EXPLODE, field: {field: abc, type: null}, keyAlias: foo, valueAlias: null}");
    }
}
