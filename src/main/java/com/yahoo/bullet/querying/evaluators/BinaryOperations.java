/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the project for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.query.expressions.Operation;
import com.yahoo.bullet.record.BulletRecord;
import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Binary operations used by BinaryEvaluator.
 */
public class BinaryOperations {
    @FunctionalInterface
    public interface BinaryOperator extends Serializable {
        TypedObject apply(Evaluator left, Evaluator right, BulletRecord record);
    }

    public static final Map<Operation, BinaryOperator> BINARY_OPERATORS = new EnumMap<>(Operation.class);

    static {
        BINARY_OPERATORS.put(Operation.ADD, BinaryOperations::add);
        BINARY_OPERATORS.put(Operation.SUB, BinaryOperations::sub);
        BINARY_OPERATORS.put(Operation.MUL, BinaryOperations::mul);
        BINARY_OPERATORS.put(Operation.DIV, BinaryOperations::div);
        BINARY_OPERATORS.put(Operation.EQUALS, BinaryOperations::equals);
        BINARY_OPERATORS.put(Operation.EQUALS_ANY, BinaryOperations::equalsAny);
        BINARY_OPERATORS.put(Operation.EQUALS_ALL, BinaryOperations::equalsAll);
        BINARY_OPERATORS.put(Operation.NOT_EQUALS, BinaryOperations::notEquals);
        BINARY_OPERATORS.put(Operation.NOT_EQUALS_ANY, BinaryOperations::notEqualsAny);
        BINARY_OPERATORS.put(Operation.NOT_EQUALS_ALL, BinaryOperations::notEqualsAll);
        BINARY_OPERATORS.put(Operation.GREATER_THAN, BinaryOperations::greaterThan);
        BINARY_OPERATORS.put(Operation.GREATER_THAN_ANY, BinaryOperations::greaterThanAny);
        BINARY_OPERATORS.put(Operation.GREATER_THAN_ALL, BinaryOperations::greaterThanAll);
        BINARY_OPERATORS.put(Operation.LESS_THAN, BinaryOperations::lessThan);
        BINARY_OPERATORS.put(Operation.LESS_THAN_ANY, BinaryOperations::lessThanAny);
        BINARY_OPERATORS.put(Operation.LESS_THAN_ALL, BinaryOperations::lessThanAll);
        BINARY_OPERATORS.put(Operation.GREATER_THAN_OR_EQUALS, BinaryOperations::greaterThanOrEquals);
        BINARY_OPERATORS.put(Operation.GREATER_THAN_OR_EQUALS_ANY, BinaryOperations::greaterThanOrEqualsAny);
        BINARY_OPERATORS.put(Operation.GREATER_THAN_OR_EQUALS_ALL, BinaryOperations::greaterThanOrEqualsAll);
        BINARY_OPERATORS.put(Operation.LESS_THAN_OR_EQUALS, BinaryOperations::lessThanOrEquals);
        BINARY_OPERATORS.put(Operation.LESS_THAN_OR_EQUALS_ANY, BinaryOperations::lessThanOrEqualsAny);
        BINARY_OPERATORS.put(Operation.LESS_THAN_OR_EQUALS_ALL, BinaryOperations::lessThanOrEqualsAll);
        BINARY_OPERATORS.put(Operation.REGEX_LIKE, BinaryOperations::regexLike);
        BINARY_OPERATORS.put(Operation.REGEX_LIKE_ANY, BinaryOperations::regexLikeAny);
        BINARY_OPERATORS.put(Operation.SIZE_IS, BinaryOperations::sizeIs);
        BINARY_OPERATORS.put(Operation.CONTAINS_KEY, BinaryOperations::containsKey);
        BINARY_OPERATORS.put(Operation.CONTAINS_VALUE, BinaryOperations::containsValue);
        BINARY_OPERATORS.put(Operation.IN, BinaryOperations::in);
        BINARY_OPERATORS.put(Operation.AND, BinaryOperations::and);
        BINARY_OPERATORS.put(Operation.OR, BinaryOperations::or);
        BINARY_OPERATORS.put(Operation.XOR, BinaryOperations::xor);
        BINARY_OPERATORS.put(Operation.FILTER, BinaryOperations::filter);
    }

    static TypedObject add(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type type = getArithmeticResultType(leftValue.getType(), rightValue.getType());
        switch (type) {
            case DOUBLE:
                return new TypedObject(Type.DOUBLE, getDouble(leftValue) + getDouble(rightValue));
            case FLOAT:
                return new TypedObject(Type.FLOAT, getFloat(leftValue) + getFloat(rightValue));
            case LONG:
                return new TypedObject(Type.LONG, getLong(leftValue) + getLong(rightValue));
            default:
                return new TypedObject(Type.INTEGER, getInteger(leftValue) + getInteger(rightValue));
        }
    }

    static TypedObject sub(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type type = getArithmeticResultType(leftValue.getType(), rightValue.getType());
        switch (type) {
            case DOUBLE:
                return new TypedObject(Type.DOUBLE, getDouble(leftValue) - getDouble(rightValue));
            case FLOAT:
                return new TypedObject(Type.FLOAT, getFloat(leftValue) - getFloat(rightValue));
            case LONG:
                return new TypedObject(Type.LONG, getLong(leftValue) - getLong(rightValue));
            default:
                return new TypedObject(Type.INTEGER, getInteger(leftValue) - getInteger(rightValue));
        }
    };

    static TypedObject mul(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type type = getArithmeticResultType(leftValue.getType(), rightValue.getType());
        switch (type) {
            case DOUBLE:
                return new TypedObject(Type.DOUBLE, getDouble(leftValue) * getDouble(rightValue));
            case FLOAT:
                return new TypedObject(Type.FLOAT, getFloat(leftValue) * getFloat(rightValue));
            case LONG:
                return new TypedObject(Type.LONG, getLong(leftValue) * getLong(rightValue));
            default:
                return new TypedObject(Type.INTEGER, getInteger(leftValue) * getInteger(rightValue));
        }
    };

    static TypedObject div(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type type = getArithmeticResultType(leftValue.getType(), rightValue.getType());
        switch (type) {
            case DOUBLE:
                return new TypedObject(Type.DOUBLE, getDouble(leftValue) / getDouble(rightValue));
            case FLOAT:
                return new TypedObject(Type.FLOAT, getFloat(leftValue) / getFloat(rightValue));
            case LONG:
                return new TypedObject(Type.LONG, getLong(leftValue) / getLong(rightValue));
            default:
                return new TypedObject(Type.INTEGER, getInteger(leftValue) / getInteger(rightValue));
        }
    };

    static TypedObject equals(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.equalTo(rightValue));
    };

    static TypedObject equalsAny(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = rightValue.getType().getSubType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().anyMatch(o -> leftValue.equalTo(new TypedObject(subType, o))));
    };

    static TypedObject equalsAll(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = rightValue.getType().getSubType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().allMatch(o -> leftValue.equalTo(new TypedObject(subType, o))));
    };

    static TypedObject notEquals(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, !leftValue.equalTo(rightValue));
    };

    static TypedObject notEqualsAny(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = rightValue.getType().getSubType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().anyMatch(o -> !leftValue.equalTo(new TypedObject(subType, o))));
    };

    static TypedObject notEqualsAll(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = rightValue.getType().getSubType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().noneMatch(o -> leftValue.equalTo(new TypedObject(subType, o))));
    };

    static TypedObject greaterThan(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.compareTo(rightValue) > 0);
    };

    static TypedObject greaterThanAny(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = rightValue.getType().getSubType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().anyMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) > 0));
    };

    static TypedObject greaterThanAll(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = rightValue.getType().getSubType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().allMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) > 0));
    };

    static TypedObject lessThan(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.compareTo(rightValue) < 0);
    };

    static TypedObject lessThanAny(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = rightValue.getType().getSubType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().anyMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) < 0));
    };

    static TypedObject lessThanAll(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = rightValue.getType().getSubType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().allMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) < 0));
    };

    static TypedObject greaterThanOrEquals(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.compareTo(rightValue) >= 0);
    };

    static TypedObject greaterThanOrEqualsAny(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = rightValue.getType().getSubType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().anyMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) >= 0));
    };

    static TypedObject greaterThanOrEqualsAll(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = rightValue.getType().getSubType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().allMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) >= 0));
    };

    static TypedObject lessThanOrEquals(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.compareTo(rightValue) <= 0);
    };

    static TypedObject lessThanOrEqualsAny(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = rightValue.getType().getSubType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().anyMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) <= 0));
    };

    static TypedObject lessThanOrEqualsAll(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = rightValue.getType().getSubType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().allMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) <= 0));
    };

    static TypedObject regexLike(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, Pattern.compile((String) rightValue.getValue()).matcher((String) leftValue.getValue()).matches());
    };

    static TypedObject regexLikeAny(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        String value = (String) leftValue.getValue();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().map(o -> Pattern.compile((String) o).matcher(value)).anyMatch(Matcher::matches));
    };

    static TypedObject sizeIs(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.size() == (int) rightValue.getValue());
    };

    static TypedObject containsKey(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.containsKey((String) rightValue.getValue()));
    };

    static TypedObject containsValue(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.containsValue(rightValue));
    };

    static TypedObject in(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, rightValue.containsValue(leftValue));
    };

    static TypedObject and(Evaluator left, Evaluator right, BulletRecord record) {
        return new TypedObject(Type.BOOLEAN, (Boolean) left.evaluate(record).forceCast(Type.BOOLEAN).getValue() &&
                                             (Boolean) right.evaluate(record).forceCast(Type.BOOLEAN).getValue());
    }

    static TypedObject or(Evaluator left, Evaluator right, BulletRecord record) {
        return new TypedObject(Type.BOOLEAN, (Boolean) left.evaluate(record).forceCast(Type.BOOLEAN).getValue() ||
                                             (Boolean) right.evaluate(record).forceCast(Type.BOOLEAN).getValue());
    }

    static TypedObject xor(Evaluator left, Evaluator right, BulletRecord record) {
        return new TypedObject(Type.BOOLEAN, (Boolean) left.evaluate(record).forceCast(Type.BOOLEAN).getValue() ^
                                             (Boolean) right.evaluate(record).forceCast(Type.BOOLEAN).getValue());
    }

    static TypedObject filter(Evaluator left, Evaluator right, BulletRecord record) {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        List<? extends Serializable> list = (List<? extends Serializable>) leftValue.getValue();
        List<Boolean> booleans = (List<Boolean>) rightValue.getValue();
        return new TypedObject(leftValue.getType(), IntStream.range(0, list.size()).filter(booleans::get).mapToObj(list::get).collect(Collectors.toCollection(ArrayList::new)));
    };

    private static Type getArithmeticResultType(Type left, Type right) {
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

    private static Integer getInteger(TypedObject object) {
        return ((Number) object.getValue()).intValue();
    }

    private static Long getLong(TypedObject object) {
        return ((Number) object.getValue()).longValue();
    }

    private static Float getFloat(TypedObject object) {
        return ((Number) object.getValue()).floatValue();
    }

    private static Double getDouble(TypedObject object) {
        return ((Number) object.getValue()).doubleValue();
    }
}
