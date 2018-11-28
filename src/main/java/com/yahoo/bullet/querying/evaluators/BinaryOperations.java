package com.yahoo.bullet.querying.evaluators;

import com.yahoo.bullet.typesystem.Type;
import com.yahoo.bullet.typesystem.TypedObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BinaryOperations {

    static BinaryOperator<TypedObject> ADD = (left, right) -> {
        Type type = getResultType(left.getType(), right.getType());
        switch (type) {
            case INTEGER:
                Integer i1 = (Integer) left.getValue();
                Integer i2 = (Integer) right.getValue();
                return new TypedObject(Type.INTEGER, i1 + i2);
            case LONG:
                Long l1 = (Long) left.getValue();
                Long l2 = (Long) right.getValue();
                return new TypedObject(Type.LONG, l1 + l2);
            case FLOAT:
                Float f1 = (Float) left.getValue();
                Float f2 = (Float) right.getValue();
                return new TypedObject(Type.FLOAT, f1 + f2);
            case DOUBLE:
                Double d1 = (Double) left.getValue();
                Double d2 = (Double) right.getValue();
                return new TypedObject(Type.DOUBLE, d1 + d2);
        }
        throw new UnsupportedOperationException("'+' operands must have numeric type.");
    };

    static BinaryOperator<TypedObject> SUB = (left, right) -> {
        Type type = getResultType(left.getType(), right.getType());
        switch (type) {
            case INTEGER:
                Integer i1 = (Integer) left.getValue();
                Integer i2 = (Integer) right.getValue();
                return new TypedObject(Type.INTEGER, i1 - i2);
            case LONG:
                Long l1 = (Long) left.getValue();
                Long l2 = (Long) right.getValue();
                return new TypedObject(Type.LONG, l1 - l2);
            case FLOAT:
                Float f1 = (Float) left.getValue();
                Float f2 = (Float) right.getValue();
                return new TypedObject(Type.FLOAT, f1 - f2);
            case DOUBLE:
                Double d1 = (Double) left.getValue();
                Double d2 = (Double) right.getValue();
                return new TypedObject(Type.DOUBLE, d1 - d2);
        }
        throw new UnsupportedOperationException("'-' operands must have numeric type.");
    };

    static BinaryOperator<TypedObject> MUL = (left, right) -> {
        Type type = getResultType(left.getType(), right.getType());
        switch (type) {
            case INTEGER:
                Integer i1 = (Integer) left.getValue();
                Integer i2 = (Integer) right.getValue();
                return new TypedObject(Type.INTEGER, i1 * i2);
            case LONG:
                Long l1 = (Long) left.getValue();
                Long l2 = (Long) right.getValue();
                return new TypedObject(Type.LONG, l1 * l2);
            case FLOAT:
                Float f1 = (Float) left.getValue();
                Float f2 = (Float) right.getValue();
                return new TypedObject(Type.FLOAT, f1 * f2);
            case DOUBLE:
                Double d1 = (Double) left.getValue();
                Double d2 = (Double) right.getValue();
                return new TypedObject(Type.DOUBLE, d1 * d2);
        }
        throw new UnsupportedOperationException("'*' operands must have numeric type.");
    };

    static BinaryOperator<TypedObject> DIV = (left, right) -> {
        Type type = getResultType(left.getType(), right.getType());
        switch (type) {
            case INTEGER:
                Integer i1 = (Integer) left.getValue();
                Integer i2 = (Integer) right.getValue();
                return new TypedObject(Type.INTEGER, i1 / i2);
            case LONG:
                Long l1 = (Long) left.getValue();
                Long l2 = (Long) right.getValue();
                return new TypedObject(Type.LONG, l1 / l2);
            case FLOAT:
                Float f1 = (Float) left.getValue();
                Float f2 = (Float) right.getValue();
                return new TypedObject(Type.FLOAT, f1 / f2);
            case DOUBLE:
                Double d1 = (Double) left.getValue();
                Double d2 = (Double) right.getValue();
                return new TypedObject(Type.DOUBLE, d1 / d2);
        }
        throw new UnsupportedOperationException("'/' operands must have numeric type.");
    };

    static BinaryOperator<TypedObject> EQUALS = (left, right) -> {
        Type leftType = left.getType();
        Type rightType = right.getType();
        if (!Type.PRIMITIVES.contains(leftType)) {
            throw new UnsupportedOperationException("'==' left operand must have primitive type.");
        }
        if (leftType == rightType) {
            return new TypedObject(left.equalTo(right));
        }
        if (rightType == Type.LIST && leftType == right.getPrimitiveType()) {
            return new TypedObject(((List<Object>) right.getValue()).stream().anyMatch(left::equalTo));
        }
        throw new UnsupportedOperationException("'==' right operand must have corresponding type or have type LIST with corresponding primitive type.");
    };

    static BinaryOperator<TypedObject> NOT_EQUALS = (left, right) -> {
        Type leftType = left.getType();
        Type rightType = right.getType();
        if (!Type.PRIMITIVES.contains(leftType)) {
            throw new UnsupportedOperationException("'!=' left operand must have primitive type.");
        }
        if (leftType == rightType) {
            return new TypedObject(!left.equalTo(right));
        }
        if (rightType == Type.LIST && leftType == right.getPrimitiveType()) {
            return new TypedObject(((List<Object>) right.getValue()).stream().noneMatch(left::equalTo));
        }
        throw new UnsupportedOperationException("'!=' right operand must have corresponding type or have type LIST with corresponding primitive type.");
    };

    static BinaryOperator<TypedObject> GREATER_THAN = (left, right) -> {
        Type leftType = left.getType();
        Type rightType = right.getType();
        if (!Type.PRIMITIVES.contains(leftType)) {
            throw new UnsupportedOperationException("'>' left operand must have primitive type.");
        }
        if (leftType == rightType) {
            return new TypedObject(left.compareTo(right) > 0);
        }
        if (rightType == Type.LIST && leftType == right.getPrimitiveType()) {
            return new TypedObject(((List<Object>) right.getValue()).stream().anyMatch(o -> left.compareTo(new TypedObject(o)) > 0));
        }
        throw new UnsupportedOperationException("'>' right operand must have corresponding type or have type LIST with corresponding primitive type.");
    };

    static BinaryOperator<TypedObject> LESS_THAN = (left, right) -> {
        Type leftType = left.getType();
        Type rightType = right.getType();
        if (!Type.PRIMITIVES.contains(leftType)) {
            throw new UnsupportedOperationException("'<' left operand must have primitive type.");
        }
        if (leftType == rightType) {
            return new TypedObject(left.compareTo(right) < 0);
        }
        if (rightType == Type.LIST && leftType == right.getPrimitiveType()) {
            return new TypedObject(((List<Object>) right.getValue()).stream().anyMatch(o -> left.compareTo(new TypedObject(o)) < 0));
        }
        throw new UnsupportedOperationException("'<' right operand must have corresponding type or have type LIST with corresponding primitive type.");
    };

    static BinaryOperator<TypedObject> GREATER_THAN_OR_EQUALS = (left, right) -> {
        Type leftType = left.getType();
        Type rightType = right.getType();
        if (!Type.PRIMITIVES.contains(leftType)) {
            throw new UnsupportedOperationException("'>=' left operand must have primitive type.");
        }
        if (leftType == rightType) {
            return new TypedObject(left.compareTo(right) >= 0);
        }
        if (rightType == Type.LIST && leftType == right.getPrimitiveType()) {
            return new TypedObject(((List<Object>) right.getValue()).stream().anyMatch(o -> left.compareTo(new TypedObject(o)) >= 0));
        }
        throw new UnsupportedOperationException("'>=' right operand must have corresponding type or have type LIST with corresponding primitive type.");
    };

    static BinaryOperator<TypedObject> LESS_THAN_OR_EQUALS = (left, right) -> {
        Type leftType = left.getType();
        Type rightType = right.getType();
        if (!Type.PRIMITIVES.contains(leftType)) {
            throw new UnsupportedOperationException("'<=' left operand must have primitive type.");
        }
        if (leftType == rightType) {
            return new TypedObject(left.compareTo(right) <= 0);
        }
        if (rightType == Type.LIST && leftType == right.getPrimitiveType()) {
            return new TypedObject(((List<Object>) right.getValue()).stream().anyMatch(o -> left.compareTo(new TypedObject(o)) <= 0));
        }
        throw new UnsupportedOperationException("'<=' right operand must have corresponding type or have type LIST with corresponding primitive type.");
    };

    static BinaryOperator<TypedObject> REGEX_LIKE = (left, right) -> {
        Type leftType = left.getType();
        Type rightType = right.getType();
        if (leftType != Type.STRING) {
            throw new UnsupportedOperationException("'RLIKE' left operand must have type STRING.");
        }
        if (rightType == Type.STRING) {
            return new TypedObject(Pattern.compile((String) right.getValue()).matcher((String) left.getValue()).matches());
        }
        if (rightType == Type.LIST && leftType == right.getPrimitiveType()) {
            String value = (String) left.getValue();
            return new TypedObject(((List<String>) right.getValue()).stream().map(Pattern::compile).map(p -> p.matcher(value)).anyMatch(Matcher::matches));
        }
        throw new UnsupportedOperationException("'RLIKE' right operand must have type STRING or have type LIST with primitive type STRING.");
    };

    static BinaryOperator<TypedObject> SIZE_IS = (left, right) -> {
        TypedObject size = new TypedObject(left.size());
        if (right.getType() == Type.LIST) {
            return new TypedObject(((List<Object>) right.getValue()).stream().anyMatch(size::equalTo));
        }
        return new TypedObject(right.equalTo(size));
    };

    static BinaryOperator<TypedObject> CONTAINS_KEY = (left, right) -> {
        Type rightType = right.getType();
        if (rightType == Type.STRING) {
            return new TypedObject(left.containsKey((String) right.getValue()));
        }
        if (rightType == Type.LIST && right.getPrimitiveType() == Type.STRING) {
            return new TypedObject(((List<String>) right.getValue()).stream().anyMatch(left::containsKey));
        }
        throw new UnsupportedOperationException("'CONTAINSKEY' right operand must have type STRING or have type LIST with primitive type STRING.");
    };

    static BinaryOperator<TypedObject> CONTAINS_VALUE = (left, right) -> {
        if (right.getType() == Type.LIST) {
            return new TypedObject(((List<Object>) right.getValue()).stream().map(TypedObject::new).anyMatch(left::containsValue));
        }
        return new TypedObject(left.containsValue(right));
    };

    static BinaryOperator<TypedObject> AND = (left, right) -> {
        return new TypedObject((Boolean) left.forceCast(Type.BOOLEAN).getValue() && (Boolean) right.forceCast(Type.BOOLEAN).getValue());
    };

    static BinaryOperator<TypedObject> OR = (left, right) -> {
        return new TypedObject((Boolean) left.forceCast(Type.BOOLEAN).getValue() || (Boolean) right.forceCast(Type.BOOLEAN).getValue());
    };

    static BinaryOperator<TypedObject> XOR = (left, right) -> {
        return new TypedObject((Boolean) left.forceCast(Type.BOOLEAN).getValue() ^ (Boolean) right.forceCast(Type.BOOLEAN).getValue());
    };

    static BinaryOperator<TypedObject> FILTER = (left, right) -> {
        if (left.getType() != Type.LIST || right.getType() != Type.LIST || left.size() != right.size()) {
            throw new UnsupportedOperationException("'FILTER' operands must be two lists of the same size.");
        }
        List<Object> objects = (List<Object>) left.getValue();
        List<Boolean> filter = ((List<Object>) right.getValue()).stream().map(o -> (Boolean) TypedObject.typeCastFromObject(Type.BOOLEAN, o).getValue()).collect(Collectors.toList());
        return new TypedObject(IntStream.range(0, left.size()).mapToObj(i -> filter.get(i) ? objects.get(i) : null).filter(Objects::nonNull).collect(Collectors.toList()));
    };

    private static Type getResultType(Type left, Type right) {
        if (!Type.NUMERICS.contains(left) || !Type.NUMERICS.contains(right)) {
            return Type.NULL;
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
