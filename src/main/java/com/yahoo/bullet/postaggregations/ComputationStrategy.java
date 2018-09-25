/*
 *  Copyright 2018, Oath Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.postaggregations;

import com.yahoo.bullet.parsing.BinaryExpression;
import com.yahoo.bullet.parsing.Computation;
import com.yahoo.bullet.parsing.Expression;
import com.yahoo.bullet.parsing.LeafExpression;
import com.yahoo.bullet.parsing.Value;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.result.Clip;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

import static com.yahoo.bullet.common.Utilities.extractTypedObject;

@Slf4j @AllArgsConstructor
public class ComputationStrategy implements PostStrategy {
    private Computation postAggregation;

    @Override
    public Clip execute(Clip clip) {
        clip.getRecords().forEach(r -> {
                try {
                    TypedObject result = calculate(postAggregation.getExpression(), r);
                    switch (result.getType()) {
                        case INTEGER:
                            r.setInteger(postAggregation.getNewName(), (Integer) result.getValue());
                            break;
                        case LONG:
                            r.setLong(postAggregation.getNewName(), (Long) result.getValue());
                            break;
                        case DOUBLE:
                            r.setDouble(postAggregation.getNewName(), (Double) result.getValue());
                            break;
                        case FLOAT:
                            r.setFloat(postAggregation.getNewName(), (Float) result.getValue());
                            break;
                        case BOOLEAN:
                            r.setBoolean(postAggregation.getNewName(), (Boolean) result.getValue());
                            break;
                        case STRING:
                            r.setString(postAggregation.getNewName(), (String) result.getValue());
                            break;
                    }
                } catch (RuntimeException e) {
                    // Ignore the exception and skip setting the field.
                    log.error("Unable to calculate the expression: " + postAggregation.getExpression());
                    log.error("Skip it due to: " + e);
                }
            });
        return clip;
    }

    @Override
    public Set<String> getRequiredFields() {
        return postAggregation.getExpression().getRequiredFields();
    }

    private TypedObject calculateLeafExpression(LeafExpression expression, BulletRecord record) {
        Value value = expression.getValue();
        TypedObject result = null;
        String valueString = value.getValue();
        Type type = value.getType();
        switch (value.getKind()) {
            case FIELD:
                result = extractTypedObject(valueString, record);
                if (type != null) {
                    result = result.forceCast(type);
                }
                break;
            case VALUE:
                result = TypedObject.forceCast(type, valueString);
                break;
        }
        return result;
    }

    private TypedObject calculateBinaryExpression(BinaryExpression expression, BulletRecord record) {
        TypedObject left = calculate(expression.getLeft(), record);
        TypedObject right = calculate(expression.getRight(), record);
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

        Type newType = expression.getType();
        if (newType != null) {
            result = result.forceCast(newType);
        }
        return result;
    }

    private TypedObject calculate(Expression expression, BulletRecord record) {
        // Rather than define another hierarchy of Expression -> LeafExpression or BinaryExpression calculators, we'll eat the
        // cost of violating polymorphism in this one spot.
        // We do not want processing logic in LeafExpression or BinaryExpression, otherwise we could put the appropriate
        // methods in those classes.
        if (expression instanceof LeafExpression) {
            return calculateLeafExpression((LeafExpression) expression, record);
        } else {
            return calculateBinaryExpression((BinaryExpression) expression, record);
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
