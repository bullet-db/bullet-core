/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.query.expressions;

import org.testng.Assert;

import java.util.function.Supplier;

public class ExpressionUtils {
    /**
     * Helper for testing equals() and hashCode() in classes that extend {@link Expression}.
     *
     * @param supplier A supplier that constructs the expression to compare to.
     * @param expressions The other expressions to compare to that should be not equal.
     */
    public static void testEqualsAndHashCode(Supplier<Expression> supplier, Expression... expressions) {
        Expression expression = supplier.get();
        Assert.assertEquals(expression, expression);
        Assert.assertEquals(expression.hashCode(), expression.hashCode());

        for (Expression other : expressions) {
            Assert.assertNotEquals(expression, other);
            Assert.assertNotEquals(expression.hashCode(), other.hashCode());
        }

        Expression other = supplier.get();
        Assert.assertEquals(expression, other);
        Assert.assertEquals(expression.hashCode(), other.hashCode());

        // coverage
        Assert.assertFalse(expression.equals(null));
    }
}
