/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.common.BulletError;
import com.yahoo.bullet.parsing.BinaryExpression;
import com.yahoo.bullet.parsing.CastExpression;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.parsing.Expression;
import com.yahoo.bullet.parsing.LeafExpression;
import com.yahoo.bullet.parsing.Value;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Optional;

import static com.yahoo.bullet.common.Utilities.extractTypedObject;

@AllArgsConstructor
public class ComputationStrategy implements PostStrategy {
    private Computation postAggregation;

    @Override
    public Clip execute(Clip clip) {
        clip.getRecords().forEach(r -> {
                try {
                    TypedObject result = calculate(postAggregation.getExpression(), r);
                    switch (result.getType()) {
                        case INTEGER:
                            r.setInteger(postAggregation.getNewFieldName(), (Integer) result.getValue());
                            break;
                        case LONG:
                            r.setLong(postAggregation.getNewFieldName(), (Long) result.getValue());
                            break;
                        case DOUBLE:
                            r.setDouble(postAggregation.getNewFieldName(), (Double) result.getValue());
                            break;
                        case FLOAT:
                            r.setFloat(postAggregation.getNewFieldName(), (Float) result.getValue());
                            break;
                        case BOOLEAN:
                            r.setBoolean(postAggregation.getNewFieldName(), (Boolean) result.getValue());
                            break;
                        case STRING:
                            r.setString(postAggregation.getNewFieldName(), (String) result.getValue());
                            break;
                        default:
                            r.setString(postAggregation.getNewFieldName(), "N/A");
                    }
                } catch (RuntimeException e) {
                    r.setString(postAggregation.getNewFieldName(), "N/A");
                }
            });
        return clip;
    }

    @Override
    public Optional<List<BulletError>> initialize() {
        return Optional.empty();
    }

    private TypedObject calculateLeafExpression(LeafExpression expression, BulletRecord record) {
        Value value = expression.getValue();
        TypedObject result = null;
        String valueString = value.getValue();
        switch (value.getKind()) {
            case FIELD:
                result = extractTypedObject(valueString, record);
                break;
            case VALUE:
                if (valueString.equals("true") || valueString.equals("false")) {
                    result = TypedObject.typeCast(Type.BOOLEAN, valueString);
                } else if (valueString.matches("^'.*'$")) {
                    result = TypedObject.typeCast(Type.STRING, valueString.substring(1, valueString.length() - 1));
                } else if (valueString.contains(".")) {
                    result = TypedObject.typeCast(Type.DOUBLE, valueString);
                } else {
                    result = TypedObject.typeCast(Type.LONG, valueString);
                }
                break;
        }
        return result;
    }

    private TypedObject calculateBinaryExpression(BinaryExpression expression, BulletRecord record) {
        TypedObject left = calculate(expression.getLeftExpression(), record);
        TypedObject right = calculate(expression.getRightExpression(), record);
        Type resultType = getResultType(left.getType(), right.getType());
        TypedObject castedLeft = left.forceCast(resultType);
        TypedObject castedRight = right.forceCast(resultType);

        TypedObject result = null;
        switch (expression.getOperation()) {
            case ADD:
                result = add(resultType, castedLeft, castedRight);
                break;
            case SUB:
                result = sub(resultType, castedLeft, castedRight);
                break;
            case MUL:
                result = mul(resultType, castedLeft, castedRight);
                break;
            case DIV:
                result = div(resultType, castedLeft, castedRight);
                break;
        }
        return result;
    }

    private TypedObject calculateCastExpression(CastExpression expression, BulletRecord record) {
        TypedObject value = calculate(expression.getExpression(), record);
        String typeString = expression.getType().name();
        return value.forceCast(Type.valueOf(typeString));
    }

    private TypedObject calculate(Expression expression, BulletRecord record) {
        // Rather than define another hierarchy of Expression -> LeafExpression, BinaryExpression, CastExpression calculators, we'll eat the
        // cost of violating polymorphism in this one spot.
        // We do not want processing logic in LeafExpression, BinaryExpression or CastExpression, otherwise we could put the appropriate
        // methods in those classes.
        if (expression instanceof LeafExpression) {
            return calculateLeafExpression((LeafExpression) expression, record);
        } else if (expression instanceof BinaryExpression) {
            return calculateBinaryExpression((BinaryExpression) expression, record);
        } else {
            return calculateCastExpression((CastExpression) expression, record);
        }
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

    private TypedObject add(Type type, TypedObject o1, TypedObject o2) {
        TypedObject result = null;
        switch (type) {
            case INTEGER:
                Integer i1 = (Integer) o1.getValue();
                Integer i2 = (Integer) o2.getValue();
                result = new TypedObject(Type.INTEGER, i1 + i2);
                break;
            case LONG:
                Long l1 = (Long) o1.getValue();
                Long l2 = (Long) o2.getValue();
                result = new TypedObject(Type.LONG, l1 + l2);
                break;
            case FLOAT:
                Float f1 = (Float) o1.getValue();
                Float f2 = (Float) o2.getValue();
                result = new TypedObject(Type.FLOAT, f1 + f2);
                break;
            case DOUBLE:
                Double d1 = (Double) o1.getValue();
                Double d2 = (Double) o2.getValue();
                result = new TypedObject(Type.DOUBLE, d1 + d2);
                break;
        }
        return result;
    }

    private TypedObject sub(Type type, TypedObject o1, TypedObject o2) {
        TypedObject result = null;
        switch (type) {
            case INTEGER:
                Integer i1 = (Integer) o1.getValue();
                Integer i2 = (Integer) o2.getValue();
                result = new TypedObject(Type.INTEGER, i1 - i2);
                break;
            case LONG:
                Long l1 = (Long) o1.getValue();
                Long l2 = (Long) o2.getValue();
                result = new TypedObject(Type.LONG, l1 - l2);
                break;
            case FLOAT:
                Float f1 = (Float) o1.getValue();
                Float f2 = (Float) o2.getValue();
                result = new TypedObject(Type.FLOAT, f1 - f2);
                break;
            case DOUBLE:
                Double d1 = (Double) o1.getValue();
                Double d2 = (Double) o2.getValue();
                result = new TypedObject(Type.DOUBLE, d1 - d2);
                break;
        }
        return result;
    }

    private TypedObject mul(Type type, TypedObject o1, TypedObject o2) {
        TypedObject result = null;
        switch (type) {
            case INTEGER:
                Integer i1 = (Integer) o1.getValue();
                Integer i2 = (Integer) o2.getValue();
                result = new TypedObject(Type.INTEGER, i1 * i2);
                break;
            case LONG:
                Long l1 = (Long) o1.getValue();
                Long l2 = (Long) o2.getValue();
                result = new TypedObject(Type.LONG, l1 * l2);
                break;
            case FLOAT:
                Float f1 = (Float) o1.getValue();
                Float f2 = (Float) o2.getValue();
                result = new TypedObject(Type.FLOAT, f1 * f2);
                break;
            case DOUBLE:
                Double d1 = (Double) o1.getValue();
                Double d2 = (Double) o2.getValue();
                result = new TypedObject(Type.DOUBLE, d1 * d2);
                break;
        }
        return result;
    }

    private TypedObject div(Type type, TypedObject o1, TypedObject o2) {
        TypedObject result = null;
        switch (type) {
            case INTEGER:
                Integer i1 = (Integer) o1.getValue();
                Integer i2 = (Integer) o2.getValue();
                result = new TypedObject(Type.INTEGER, i1 / i2);
                break;
            case LONG:
                Long l1 = (Long) o1.getValue();
                Long l2 = (Long) o2.getValue();
                result = new TypedObject(Type.LONG, l1 / l2);
                break;
            case FLOAT:
                Float f1 = (Float) o1.getValue();
                Float f2 = (Float) o2.getValue();
                result = new TypedObject(Type.FLOAT, f1 / f2);
                break;
            case DOUBLE:
                Double d1 = (Double) o1.getValue();
                Double d2 = (Double) o2.getValue();
                result = new TypedObject(Type.DOUBLE, d1 / d2);
                break;
        }
        return result;
    }
}
