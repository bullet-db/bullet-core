/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.parsing;

import com.yahoo.bullet.typesystem.Type;

public class ExpressionUtils {
    public static Expression makeLeafExpression(Value value) {
        LeafExpression expression = new LeafExpression();
        expression.setValue(value);
        return expression;
    }

    public static Expression makeCastExpression(Expression expression, Type type) {
        CastExpression castExpression = new CastExpression();
        castExpression.setOperation(Expression.Operation.CAST);
        castExpression.setExpression(expression);
        castExpression.setType(type);
        return castExpression;
    }

    public static Expression makeBinaryExpression(Expression.Operation op, Expression leftExpression, Expression rightExpression) {
        BinaryExpression binaryExpression = new BinaryExpression();
        binaryExpression.setOperation(op);
        binaryExpression.setLeft(leftExpression);
        binaryExpression.setRight(rightExpression);
        return binaryExpression;
    }
}
