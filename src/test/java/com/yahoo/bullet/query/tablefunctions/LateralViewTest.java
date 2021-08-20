/*
 *  Copyright 2021, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.tablefunctions;

import com.yahoo.bullet.query.expressions.FieldExpression;
import com.yahoo.bullet.querying.tablefunctors.LateralViewFunctor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LateralViewTest {
    @Test
    public void testLateralViewTableFunction() {
        Explode explode = new Explode(new FieldExpression("abc"), "foo", "bar", true);
        LateralView tableFunction = new LateralView(explode);

        Assert.assertEquals(tableFunction.getType(), TableFunctionType.LATERAL_VIEW);
        Assert.assertEquals(tableFunction.getTableFunctions().get(0), explode);
        Assert.assertTrue(tableFunction.getTableFunctor() instanceof LateralViewFunctor);
    }

    @Test
    public void testToString() {
        LateralView tableFunction = new LateralView(new Explode(new FieldExpression("abc"), "foo", null, true));

        Assert.assertEquals(tableFunction.toString(), "{type: LATERAL_VIEW, tableFunctions: [{outer: true, type: EXPLODE, field: {field: abc, type: null}, keyAlias: foo, valueAlias: null}]}");
    }
}
