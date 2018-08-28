/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.expressionparser;

import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.yahoo.bullet.common.Utilities.extractTypedObject;

@Slf4j @AllArgsConstructor
public class TypedExpressionVisitor extends ExpressionBaseVisitor<TypedObject> {
    private BulletRecord bulletRecord;

    @Override
    public TypedObject visitCastExpression(ExpressionParser.CastExpressionContext ctx) {
        TypedObject value = visit(ctx.expression());
        String typeString = ctx.type().getText().toUpperCase();
        return value.forceCast(Type.valueOf(typeString));
    }

    @Override
    public TypedObject visitMulDivExpression(ExpressionParser.MulDivExpressionContext ctx) {
        TypedObject left = visit(ctx.expression(0));
        TypedObject right = visit(ctx.expression(1));
        Type resultType = getResultType(left.getType(), right.getType());
        TypedObject castedLeft = left.forceCast(resultType);
        TypedObject castedRight = right.forceCast(resultType);

        String op = ctx.op.getText();
        TypedObject result = null;
        switch (resultType) {
            case INTEGER:
                Integer i1 = (Integer) castedLeft.getValue();
                Integer i2 = (Integer) castedRight.getValue();
                result = new TypedObject(Type.INTEGER, op.equals("*") ? i1 * i2 : i1 / i2);
                break;
            case LONG:
                Long l1 = (Long) castedLeft.getValue();
                Long l2 = (Long) castedRight.getValue();
                result = new TypedObject(Type.LONG, op.equals("*") ? l1 * l2 : l1 / l2);
                break;
            case FLOAT:
                Float f1 = (Float) castedLeft.getValue();
                Float f2 = (Float) castedRight.getValue();
                result = new TypedObject(Type.FLOAT, op.equals("*") ? f1 * f2 : f1 / f2);
                break;
            case DOUBLE:
                Double d1 = (Double) castedLeft.getValue();
                Double d2 = (Double) castedRight.getValue();
                result = new TypedObject(Type.DOUBLE, op.equals("*") ? d1 * d2 : d1 / d2);
                break;
        }
        return result;
    }

    @Override
    public TypedObject visitAddSubExpression(ExpressionParser.AddSubExpressionContext ctx) {
        TypedObject left = visit(ctx.expression(0));
        TypedObject right = visit(ctx.expression(1));
        Type resultType = getResultType(left.getType(), right.getType());
        TypedObject castedLeft = left.forceCast(resultType);
        TypedObject castedRight = right.forceCast(resultType);

        String op = ctx.op.getText();
        TypedObject result = null;
        switch (resultType) {
            case INTEGER:
                Integer i1 = (Integer) castedLeft.getValue();
                Integer i2 = (Integer) castedRight.getValue();
                result = new TypedObject(Type.INTEGER, op.equals("+") ? i1 + i2 : i1 - i2);
                break;
            case LONG:
                Long l1 = (Long) castedLeft.getValue();
                Long l2 = (Long) castedRight.getValue();
                result = new TypedObject(Type.LONG, op.equals("+") ? l1 + l2 : l1 - l2);
                break;
            case FLOAT:
                Float f1 = (Float) castedLeft.getValue();
                Float f2 = (Float) castedRight.getValue();
                result = new TypedObject(Type.FLOAT, op.equals("+") ? f1 + f2 : f1 - f2);
                break;
            case DOUBLE:
                Double d1 = (Double) castedLeft.getValue();
                Double d2 = (Double) castedRight.getValue();
                result = new TypedObject(Type.DOUBLE, op.equals("+") ? d1 + d2 : d1 - d2);
                break;
        }
        return result;
    }

    @Override
    public TypedObject visitField(ExpressionParser.FieldContext ctx) {
        String filedName = ctx.referenceExpression().getText();
        return extractTypedObject(filedName, bulletRecord);
    }

    @Override
    public TypedObject visitParensExpression(ExpressionParser.ParensExpressionContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public TypedObject visitSizeofExpression(ExpressionParser.SizeofExpressionContext ctx) {
        TypedObject object = visit(ctx.field());
        return new TypedObject(Type.INTEGER, object.size());
    }

    @Override
    public TypedObject visitBooleanLiteral(ExpressionParser.BooleanLiteralContext ctx) {
        return TypedObject.typeCast(Type.BOOLEAN, ctx.bool().getText());
    }

    @Override
    public TypedObject visitStringLiteral(ExpressionParser.StringLiteralContext ctx) {
        String str = ctx.string().getText();
        return TypedObject.typeCast(Type.STRING, str.substring(1, str.length() - 1));
    }

    @Override
    public TypedObject visitArithmeticUnary(ExpressionParser.ArithmeticUnaryContext ctx) {
        TypedObject number = visit(ctx.number());
        if (ctx.operator.getText().equals("+")) {
            return number;
        } else {
            TypedObject object = null;
            switch (number.getType()) {
                case LONG:
                    object = new TypedObject(Type.LONG, -1 * (Long) number.getValue());
                    break;
                case DOUBLE:
                    object = new TypedObject(Type.DOUBLE, -1 * (Double) number.getValue());
                    break;
            }
            return object;
        }
    }

    @Override
    public TypedObject visitDecimalLiteral(ExpressionParser.DecimalLiteralContext ctx) {
        return TypedObject.typeCast(Type.DOUBLE, ctx.DECIMAL_VALUE().getText());
    }

    @Override
    public TypedObject visitDoubleLiteral(ExpressionParser.DoubleLiteralContext ctx) {
        return TypedObject.typeCast(Type.DOUBLE, ctx.DOUBLE_VALUE().getText());
    }

    @Override
    public TypedObject visitIntegerLiteral(ExpressionParser.IntegerLiteralContext ctx) {
        return TypedObject.typeCast(Type.LONG, ctx.INTEGER_VALUE().getText());
    }

    private Type getResultType(Type left, Type right) {
        if (!Type.NUMERICS.contains(left) || !Type.NUMERICS.contains(right)) {
            throw new UnsupportedOperationException("Only NUMBER types are supported for +, -, * and /");
        }

        if (left == Type.DOUBLE || right == Type.DOUBLE) {
            return Type.DOUBLE;
        }
        if (left == Type.FLOAT || right == Type.FLOAT) {
            return Type.FLOAT;
        }
        if (left == Type.LONG || right == Type.LONG) {
            return Type.LONG;
        }
        return Type.INTEGER;
    }
}
