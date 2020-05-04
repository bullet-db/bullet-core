/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.postaggregations.ComputationStrategy;
import com.yahoo.bullet.query.Field;
import com.yahoo.bullet.query.expressions.ValueExpression;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;

public class ComputationTest {
    @Test
    public void testComputation() {
        Computation computation = new Computation(Collections.singletonList(new Field("abc", new ValueExpression(1))));

        Assert.assertEquals(computation.getFields(), Collections.singletonList(new Field("abc", new ValueExpression(1))));
        Assert.assertEquals(computation.getType(), PostAggregation.Type.COMPUTATION);
        Assert.assertEquals(computation.toString(), "{type: COMPUTATION, fields: [{name: abc, value: {value: 1, type: INTEGER}}]}");
        Assert.assertTrue(computation.getPostStrategy() instanceof ComputationStrategy);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testConstructorNullFields() {
        new Computation(null);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "The COMPUTATION post-aggregation requires at least one field\\.")
    public void testConstructorMissingFields() {
        new Computation(Collections.emptyList());
    }
}
