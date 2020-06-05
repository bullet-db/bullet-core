/*
 *  Copyright 2020, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.query.postaggregations;

import com.yahoo.bullet.common.BulletException;
import com.yahoo.bullet.querying.postaggregations.HavingStrategy;
import com.yahoo.bullet.query.expressions.ValueExpression;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HavingTest {
    @Test
    public void testHaving() {
        Having having = new Having(new ValueExpression(true));

        Assert.assertEquals(having.getExpression(), new ValueExpression(true));
        Assert.assertEquals(having.getType(), PostAggregationType.HAVING);
        Assert.assertEquals(having.toString(), "{type: HAVING, expression: {value: true, type: BOOLEAN}}");
        Assert.assertTrue(having.getPostStrategy() instanceof HavingStrategy);
    }

    @Test(expectedExceptions = BulletException.class, expectedExceptionsMessageRegExp = "The HAVING post-aggregation requires an expression\\.")
    public void testConstructorMissingExpression() {
        new Having(null);
    }
}
