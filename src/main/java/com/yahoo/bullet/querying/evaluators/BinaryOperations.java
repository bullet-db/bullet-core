/*
 *  Copyright 2019, Yahoo Inc.
 *  Licensed under the terms of the Apache License, Version 2.0.
 *  See the LICENSE file associated with the compute for terms.
 */
package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.yahoo.bullet.querying.evaluators.Evaluator.BinaryOperator;

/**
 * Binary operations used by BinaryEvaluator.
 */
public class BinaryOperations {
    static BinaryOperator ADD = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type type = getResultType(leftValue.getType(), rightValue.getType());
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
    };

    static BinaryOperator SUB = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type type = getResultType(leftValue.getType(), rightValue.getType());
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

    static BinaryOperator MUL = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type type = getResultType(leftValue.getType(), rightValue.getType());
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

    static BinaryOperator DIV = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type type = getResultType(leftValue.getType(), rightValue.getType());
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

    static BinaryOperator EQUALS = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.equalTo(rightValue));
    };

    static BinaryOperator EQUALS_ANY = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = leftValue.getType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().anyMatch(o -> leftValue.equalTo(new TypedObject(subType, o))));
    };

    static BinaryOperator EQUALS_ALL = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = leftValue.getType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().allMatch(o -> leftValue.equalTo(new TypedObject(subType, o))));
    };

    static BinaryOperator NOT_EQUALS = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, !leftValue.equalTo(rightValue));
    };

    static BinaryOperator NOT_EQUALS_ANY = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = leftValue.getType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().anyMatch(o -> !leftValue.equalTo(new TypedObject(subType, o))));
    };

    static BinaryOperator NOT_EQUALS_ALL = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = leftValue.getType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().noneMatch(o -> leftValue.equalTo(new TypedObject(subType, o))));
    };

    static BinaryOperator GREATER_THAN = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.compareTo(rightValue) > 0);
    };

    static BinaryOperator GREATER_THAN_ANY = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = leftValue.getType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().anyMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) > 0));
    };

    static BinaryOperator GREATER_THAN_ALL = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = leftValue.getType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().allMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) > 0));
    };

    static BinaryOperator LESS_THAN = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.compareTo(rightValue) < 0);
    };

    static BinaryOperator LESS_THAN_ANY = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = leftValue.getType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().anyMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) < 0));
    };

    static BinaryOperator LESS_THAN_ALL = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = leftValue.getType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().allMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) < 0));
    };

    static BinaryOperator GREATER_THAN_OR_EQUALS = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.compareTo(rightValue) >= 0);
    };

    static BinaryOperator GREATER_THAN_OR_EQUALS_ANY = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = leftValue.getType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().anyMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) >= 0));
    };

    static BinaryOperator GREATER_THAN_OR_EQUALS_ALL = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = leftValue.getType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().allMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) >= 0));
    };

    static BinaryOperator LESS_THAN_OR_EQUALS = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.compareTo(rightValue) <= 0);
    };

    static BinaryOperator LESS_THAN_OR_EQUALS_ANY = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = leftValue.getType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().anyMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) <= 0));
    };

    static BinaryOperator LESS_THAN_OR_EQUALS_ALL = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        Type subType = leftValue.getType();
        return new TypedObject(Type.BOOLEAN, ((List<? extends Serializable>) rightValue.getValue()).stream().allMatch(o -> leftValue.compareTo(new TypedObject(subType, o)) <= 0));
    };

    static BinaryOperator REGEX_LIKE = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, Pattern.compile((String) rightValue.getValue()).matcher((String) leftValue.getValue()).matches());
        // TODO REGEX_LIKE_ANY?
        //String value = (String) leftValue.getValue();
        //return new TypedObject(rightValue.getList().stream().map(o -> Pattern.compile((String) o).matcher(value)).anyMatch(Matcher::matches));
    };

    static BinaryOperator SIZE_IS = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.size() == (int) rightValue.getValue());
    };

    static BinaryOperator CONTAINS_KEY = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.containsKey((String) rightValue.getValue()));
    };

    static BinaryOperator CONTAINS_VALUE = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, leftValue.containsValue(rightValue));
    };

    static BinaryOperator IN = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        return new TypedObject(Type.BOOLEAN, rightValue.containsValue(leftValue));
    };

    static BinaryOperator AND = (left, right, record) ->
            new TypedObject(Type.BOOLEAN, (Boolean) left.evaluate(record).forceCast(Type.BOOLEAN).getValue() &&
                                          (Boolean) right.evaluate(record).forceCast(Type.BOOLEAN).getValue());

    static BinaryOperator OR = (left, right, record) ->
            new TypedObject(Type.BOOLEAN, (Boolean) left.evaluate(record).forceCast(Type.BOOLEAN).getValue() ||
                                          (Boolean) right.evaluate(record).forceCast(Type.BOOLEAN).getValue());

    static BinaryOperator XOR = (left, right, record) ->
            new TypedObject(Type.BOOLEAN, (Boolean) left.evaluate(record).forceCast(Type.BOOLEAN).getValue() ^
                                          (Boolean) right.evaluate(record).forceCast(Type.BOOLEAN).getValue());

    static BinaryOperator FILTER = (left, right, record) -> {
        TypedObject leftValue = left.evaluate(record);
        TypedObject rightValue = right.evaluate(record);
        List<? extends Serializable> list = (List<? extends Serializable>) leftValue.getValue();
        List<Boolean> booleans = (List<Boolean>) rightValue.getValue();
        return new TypedObject(leftValue.getType(), IntStream.range(0, list.size()).filter(booleans::get).mapToObj(list::get).collect(Collectors.toCollection(ArrayList::new)));
    };

    private static Type getResultType(Type left, Type right) {
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
